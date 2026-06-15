# VHS tape for the README failover demo.
#   vhs demo/failover.tape   →   docs/assets/failover-demo.gif
#
# Records a single Postgres connection through the gateway being preserved
# across SIGKILL of the leader. Source script: demo/failover_demo.py.

Output docs/assets/failover-demo.gif

Set FontSize 13
Set Width 900
Set Height 780
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set PlaybackSpeed 1.0

Type "./demo/failover_demo.py"
Sleep 400ms
Enter
# Demo runs ~18s plus a moment to display the final line.
Sleep 22s
