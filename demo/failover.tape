# VHS tape for the README failover demo.
#   vhs demo/failover.tape   →   docs/assets/failover-demo.gif
#
# Layout: tmux split. The top pane runs the live cluster watcher
# (pgbattery status --watch), the bottom pane drives the failover demo
# (demo/failover_demo.py), so the GIF shows the cluster state flip —
# leader dies, new leader elected, node rejoins — in the TUI while the
# client connection in the bottom pane survives the kill.
#
# Rendered at 2x the README display width: the browser downscales the
# image to the README column, which keeps text crisp on every display.

Output docs/assets/failover-demo.gif

Set FontSize 26
Set Width 1800
Set Height 1150
Set Padding 16
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set PlaybackSpeed 1.0

# Build the split off-screen: watcher already live in the top pane, prompt
# ready in the bottom pane, so the recording opens on a working TUI.
Hide
Type "tmux -f /dev/null new-session -d -s pgbdemo"
Enter
Type "tmux send-keys -t pgbdemo:0.0 './target/release/pgbattery status --nodes 127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093 --watch 1' Enter"
Enter
Type "tmux split-window -t pgbdemo -v -l 40%"
Enter
Type "tmux set -t pgbdemo status off"
Enter
Type "tmux attach -t pgbdemo"
Enter
Sleep 3s
Show

Type "./demo/failover_demo.py"
Sleep 400ms
Enter
# The demo holds a connection through leader SIGKILL (~18s), then restarts
# the killed node and exits only when the cluster is 3/3 again, so the
# recording always ends on the healed TUI.
Wait+Screen@90s /cluster healed/
Sleep 4s

Hide
Type "tmux kill-session -t pgbdemo"
Enter
