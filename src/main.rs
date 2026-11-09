use anyhow::{bail, Result};
use clap::Parser;
use riptide::client::{self, Shared, Snapshot};
use riptide::torrent;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::execute;
use rand::RngCore;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Gauge, Paragraph};
use ratatui::{Frame, Terminal};
use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Parser)]
#[command(name = "riptide", version, about = "A verified BitTorrent v1 downloader")]
struct Args {
    torrent: PathBuf,
    #[arg(short, long, default_value = "downloads")]
    output: PathBuf,
    #[arg(short, long, default_value_t = 24)]
    connections: usize,
    #[arg(long)]
    resume: Option<PathBuf>,
    #[arg(long)]
    no_tui: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !(1..=128).contains(&args.connections) {
        bail!("--connections must be between 1 and 128");
    }
    let torrent = Arc::new(torrent::Torrent::from_file(&args.torrent)?);
    let shared = Shared::open(torrent.clone(), &args.output, args.resume)?;
    let mut peer_id = [0_u8; 20];
    peer_id[..8].copy_from_slice(b"-RT0001-");
    rand::thread_rng().fill_bytes(&mut peer_id[8..]);

    if shared.is_complete() {
        println!("{}: all {} pieces verified on disk", torrent.name, torrent.piece_hashes.len());
        return Ok(());
    }

    let outcome = if args.no_tui || !io::stdout().is_terminal() || !io::stdin().is_terminal() {
        headless(shared.clone(), args.connections, peer_id).await
    } else {
        interactive(shared.clone(), args.connections, peer_id).await
    };
    shared.stop();
    let checkpoint = shared.checkpoint();
    if let Err(error) = checkpoint {
        eprintln!("Could not write final resume state: {error:#}");
    }
    let snapshot = shared.snapshot();
    println!(
        "{} · {} / {} · {}/{} pieces verified",
        snapshot.name,
        size(snapshot.verified),
        size(snapshot.total),
        snapshot.complete_pieces,
        snapshot.pieces
    );
    outcome
}

async fn headless(shared: Arc<Shared>, connections: usize, peer_id: [u8; 20]) -> Result<()> {
    let mut task = Box::pin(client::download(shared.clone(), connections, peer_id));
    loop {
        tokio::select! {
            result = &mut task => return result,
            _ = tokio::signal::ctrl_c() => {
                shared.stop();
                bail!("download stopped; rerun the same command to resume");
            }
            _ = sleep(Duration::from_secs(2)) => {
                let progress = shared.snapshot();
                println!(
                    "{:5.1}%  {:>10} / {:<10}  {:>4} peers  {}",
                    percentage(&progress),
                    size(progress.verified),
                    size(progress.total),
                    progress.connections,
                    progress.status
                );
            }
        }
    }
}

async fn interactive(shared: Arc<Shared>, connections: usize, peer_id: [u8; 20]) -> Result<()> {
    let mut terminal = Screen::open()?;
    let mut task = Box::pin(client::download(shared.clone(), connections, peer_id));
    let mut previous_bytes = 0;
    let mut previous_time = Instant::now();
    let mut transfer_rate = 0.0;

    loop {
        let snapshot = shared.snapshot();
        if previous_time.elapsed() >= Duration::from_millis(600) {
            transfer_rate = snapshot.received.saturating_sub(previous_bytes) as f64
                / previous_time.elapsed().as_secs_f64();
            previous_bytes = snapshot.received;
            previous_time = Instant::now();
        }
        terminal.terminal.draw(|frame| draw(frame, &snapshot, transfer_rate))?;
        if event::poll(Duration::from_millis(80))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q')
                    || key.code == KeyCode::Esc
                    || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
                {
                    shared.stop();
                    bail!("download paused; rerun the same command to resume");
                }
            }
        }
        tokio::select! {
            result = &mut task => return result,
            _ = tokio::signal::ctrl_c() => {
                shared.stop();
                bail!("download paused; rerun the same command to resume");
            }
            _ = sleep(Duration::from_millis(100)) => {}
        }
    }
}

struct Screen {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl Screen {
    fn open() -> Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        let terminal = match Terminal::new(CrosstermBackend::new(io::stdout())) {
            Ok(terminal) => terminal,
            Err(error) => {
                let _ = disable_raw_mode();
                let _ = execute!(io::stdout(), LeaveAlternateScreen);
                return Err(error.into());
            }
        };
        Ok(Self { terminal })
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

fn draw(frame: &mut Frame, snapshot: &Snapshot, rate: f64) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(6),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let heading = Paragraph::new(Line::from(vec![
        Span::styled("  RIPTIDE  ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(" /  BITTORRENT V1", Style::default().fg(Color::Gray)),
    ]))
    .block(Block::default().borders(Borders::ALL).title(" client "));
    frame.render_widget(heading, layout[0]);

    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title(format!(" {} ", snapshot.name)))
        .gauge_style(Style::default().fg(Color::Cyan).bg(Color::DarkGray))
        .ratio((snapshot.verified as f64 / snapshot.total.max(1) as f64).clamp(0.0, 1.0))
        .label(format!("{:.2}% verified", percentage(snapshot)));
    frame.render_widget(gauge, layout[1]);

    let metrics = Text::from(vec![
        Line::from(vec![
            Span::styled("  Verified    ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{} / {}", size(snapshot.verified), size(snapshot.total))),
        ]),
        Line::from(vec![
            Span::styled("  Pieces      ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{} / {}", snapshot.complete_pieces, snapshot.pieces)),
        ]),
        Line::from(vec![
            Span::styled("  Network     ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{} peers  ·  {}/s down  ·  {} up", snapshot.connections, size(rate as u64), size(snapshot.uploaded))),
        ]),
        Line::from(vec![
            Span::styled("  Elapsed     ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}  ·  {}", duration(snapshot.elapsed), snapshot.status)),
        ]),
    ]);
    frame.render_widget(Paragraph::new(metrics).block(Block::default().borders(Borders::ALL).title(" transfer ")), layout[2]);

    let display_lines = layout[3].height.saturating_sub(2) as usize;
    let skip = snapshot.events.len().saturating_sub(display_lines);
    let lines: Vec<Line> = snapshot.events[skip..]
        .iter()
        .map(|message| Line::from(format!("  {message}")))
        .collect();
    frame.render_widget(
        Paragraph::new(Text::from(lines)).block(Block::default().borders(Borders::ALL).title(" activity ")),
        layout[3],
    );
    frame.render_widget(
        Paragraph::new(" q / esc  pause     ctrl-c  stop     pieces checked against SHA-1 before write")
            .style(Style::default().fg(Color::DarkGray)),
        layout[4],
    );
}

fn percentage(snapshot: &Snapshot) -> f64 {
    snapshot.verified as f64 * 100.0 / snapshot.total.max(1) as f64
}

fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut quantity = bytes as f64;
    let mut unit = 0;
    while quantity >= 1024.0 && unit < UNITS.len() - 1 {
        quantity /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{quantity:.1} {}", UNITS[unit])
    }
}

fn duration(value: Duration) -> String {
    let seconds = value.as_secs();
    format!("{:02}:{:02}:{:02}", seconds / 3600, seconds / 60 % 60, seconds % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_binary_units_and_elapsed_time() {
        assert_eq!(size(512), "512 B");
        assert_eq!(size(1_572_864), "1.5 MiB");
        assert_eq!(duration(Duration::from_secs(3661)), "01:01:01");
    }
}
