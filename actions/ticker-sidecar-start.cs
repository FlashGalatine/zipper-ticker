// Streamer.bot "Ticker Sidecar Start" action (OPTIONAL) — launches the Zipper
// sidecar hidden, so SB can auto-start it. Without this you just double-click
// start-zipper.bat (or run `npm run sidecar`) once per session — but if the
// sidecar isn't running, the control page's Start / Poll / Stop / end-cap
// buttons do nothing (announcements still work, since those go SB → overlay
// without the sidecar). This action removes that footgun.
//
// HOW TO USE (README.md has the full walkthrough):
//   1. EDIT THE `BUNDLE` CONST BELOW to your Zipper repo folder (the one that holds
//      zipper-sidecar.mjs). Run `npm install` there once first (start-zipper.bat does
//      it for you): the sidecar needs its `ws` package.
//   2. Streamer.bot -> Actions -> add an action named "Ticker Sidecar Start";
//      sub-action Core -> C# -> Execute C# Code; paste everything below.
//   3. THE REFERENCES STEP (required): compiling as-is fails, first with
//        CS0234 The type or namespace name 'Net' does not exist in the namespace 'System'
//      on the `using System.Net.Sockets;` line (errors that name ProcessStartInfo,
//      Process, Stopwatch or TcpClient may follow it), because Streamer.bot 1.0.4 does not
//      reference System.dll by default. Fix: in
//      the C# editor open the References tab (next to the Compiling Log) and add
//      System.dll (browse to C:\Windows\Microsoft.NET\Framework64\v4.0.30319\System.dll
//      if it wants a full path). Then Compile -> green.
//   4. Optional: in the action's Triggers box search "start" and add your SB
//      version's application-started trigger — SB then launches the sidecar on
//      every start. No trigger? Run the action manually once per session.
//
// What it does, and logs:
//   - Something already answers on the sidecar's guard port (GUARD_PORT): most likely a
//     sidecar is running (or another program holds that port), so it starts nothing and
//     says so. After updating Zipper, close the old node process first (Task Manager),
//     then run this action again.
//   - BUNDLE does not hold zipper-sidecar.mjs: a warning that names BUNDLE.
//   - Otherwise it launches `node zipper-sidecar.mjs` in BUNDLE, hidden, and checks the
//     guard port again until it answers or LAUNCH_WAIT_MS (about 3 seconds) has passed,
//     timed by the clock (a probe of a port nothing holds takes 0.3 s on Windows). An
//     answer means only that the sidecar started and holds its guard port, not that it
//     loaded config.json or reached Streamer.bot (the control page's status line says
//     that). No answer: a warning; the sidecar may still be starting, or run
//     start-zipper.bat once to see the error (often a missing `npm install`).
//
// UseShellExecute = true is LOAD-BEARING: launching via the shell does NOT pass
// SB's open listening sockets (WS :8080 / HTTP :7474) to the node child. With
// false, an orphaned sidecar after an unclean SB exit keeps those ports bound
// and SB cannot restart its servers until the child is killed.
//
// Running it twice is harmless: it checks the guard port first, and the sidecar itself
// binds that single-instance guard port (:7496), so a second instance logs and exits.
// Requires node.exe on PATH (Node >= 20).

using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;

public class CPHInline
{
    // EDIT ME — absolute path to the Zipper repo folder (holds zipper-sidecar.mjs).
    const string BUNDLE = @"C:\path\to\zipper-ticker";

    const int GUARD_PORT = 7496; // change it here too if you start the sidecar with ZIPPER_GUARD_PORT

    // How long it waits, after the launch, for the sidecar to take its guard port.
    const int LAUNCH_WAIT_MS = 3000;

    public bool Execute()
    {
        try
        {
            // A sidecar already holds the guard port (or another program does). A second
            // one would only exit again, so say what was found instead.
            if (PortOpen(GUARD_PORT))
            {
                CPH.LogInfo("[Ticker Sidecar Start] port " + GUARD_PORT + " already answers, most likely a Zipper sidecar that is already running, so none was started. After updating Zipper, close the old node process first (Task Manager), then run this action again. If no sidecar is running, another program holds that port: start the sidecar with ZIPPER_GUARD_PORT set to a free port, and set GUARD_PORT at the top of this action to the same port.");
                return true;
            }
            if (!File.Exists(Path.Combine(BUNDLE, "zipper-sidecar.mjs")))
            {
                CPH.LogWarn("[Ticker Sidecar Start] zipper-sidecar.mjs was not found in BUNDLE = " + BUNDLE + " — edit BUNDLE at the top of this action to the Zipper folder.");
                return false;
            }
            var psi = new ProcessStartInfo();
            psi.FileName = "node";
            psi.Arguments = "zipper-sidecar.mjs";
            psi.WorkingDirectory = BUNDLE;
            psi.UseShellExecute = true; // do NOT inherit SB's listen sockets (see header)
            psi.WindowStyle = ProcessWindowStyle.Hidden;
            Process.Start(psi);
            // A hidden window hides the sidecar's own errors (an old Node, no `ws` package),
            // so check that it took its guard port: it does that first, before its config.
            // The wait is timed by the clock, not counted in probes: on Windows a probe of a
            // port nothing holds is not refused at once, so each one takes its full 0.3 s.
            bool up = false;
            var waited = Stopwatch.StartNew();
            while (!up && waited.ElapsedMilliseconds < LAUNCH_WAIT_MS) { CPH.Wait(150); up = PortOpen(GUARD_PORT); }
            // The warning names the wait it was given (LAUNCH_WAIT_MS), which the loop above has
            // just run out, not the time measured: the last probe can take it a little past.
            if (up) CPH.LogInfo("[Ticker Sidecar Start] launched node zipper-sidecar.mjs in " + BUNDLE + ": it started and holds its guard port " + GUARD_PORT + ". That does not show that it loaded its config or reached Streamer.bot: the control page's status line does.");
            else CPH.LogWarn("[Ticker Sidecar Start] node was launched in " + BUNDLE + " but nothing answers on port " + GUARD_PORT + " after about " + (LAUNCH_WAIT_MS + 500) / 1000 + " seconds. It may still be starting. If it never answers, run npm install in that folder if you have not, or run start-zipper.bat once to see the error.");
            return up;
        }
        catch (Exception ex)
        {
            CPH.LogWarn("[Ticker Sidecar Start] ERROR: " + ex.Message +
                " — is node on PATH, and is BUNDLE set to the Zipper folder?");
            return false;
        }
    }

    // True when something accepts a TCP connection on 127.0.0.1:<port> within 300 ms.
    static bool PortOpen(int port)
    {
        try
        {
            using (var c = new TcpClient())
            {
                IAsyncResult r = c.BeginConnect("127.0.0.1", port, null, null);
                bool ok = r.AsyncWaitHandle.WaitOne(300) && c.Connected;
                if (ok) c.EndConnect(r);
                return ok;
            }
        }
        catch { return false; }
    }
}
