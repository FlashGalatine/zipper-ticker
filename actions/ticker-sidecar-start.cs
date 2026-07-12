// Streamer.bot "Ticker Sidecar Start" action (OPTIONAL) — launches the Zipper
// sidecar hidden, so SB can auto-start it. Without this you just double-click
// start-zipper.bat (or run `npm run sidecar`) once per session — but if the
// sidecar isn't running, the control page's Start / Poll / Stop / end-cap
// buttons do nothing (announcements still work, since those go SB → overlay
// without the sidecar). This action removes that footgun.
//
// HOW TO USE (README.md has the full walkthrough):
//   1. EDIT THE `BUNDLE` CONST BELOW to your Zipper repo folder.
//   2. Streamer.bot -> Actions -> add an action named "Ticker Sidecar Start";
//      sub-action Core -> C# -> Execute C# Code; paste everything below.
//   3. THE REFERENCES STEP (required): compiling as-is fails with
//        CS0246 'ProcessStartInfo' could not be found / CS0103 'Process' does not exist
//      because Streamer.bot 1.0.4 does not reference System.dll by default. Fix: in
//      the C# editor open the References tab (next to the Compiling Log) and add
//      System.dll (browse to C:\Windows\Microsoft.NET\Framework64\v4.0.30319\System.dll
//      if it wants a full path). Then Compile -> green.
//   4. Optional: in the action's Triggers box search "start" and add your SB
//      version's application-started trigger — SB then launches the sidecar on
//      every start. No trigger? Run the action manually once per session.
//
// UseShellExecute = true is LOAD-BEARING: launching via the shell does NOT pass
// SB's open listening sockets (WS :8080 / HTTP :7474) to the node child. With
// false, an orphaned sidecar after an unclean SB exit keeps those ports bound
// and SB cannot restart its servers until the child is killed.
//
// Running it twice is harmless: the sidecar binds a single-instance guard port
// (:7496) — a second instance logs and exits. Requires node.exe on PATH (Node >= 18).

using System;
using System.Diagnostics;

public class CPHInline
{
    // EDIT ME — absolute path to the Zipper repo folder (holds zipper-sidecar.mjs).
    const string BUNDLE = @"D:\StreamerGraphics\StreamerBotComponents\Zipper";

    public bool Execute()
    {
        try
        {
            var psi = new ProcessStartInfo();
            psi.FileName = "node";
            psi.Arguments = "zipper-sidecar.mjs";
            psi.WorkingDirectory = BUNDLE;
            psi.UseShellExecute = true; // do NOT inherit SB's listen sockets (see header)
            psi.WindowStyle = ProcessWindowStyle.Hidden;
            Process.Start(psi);
            CPH.LogInfo("[Ticker Sidecar Start] launched node zipper-sidecar.mjs in " + BUNDLE);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogWarn("[Ticker Sidecar Start] ERROR: " + ex.Message +
                " — is node on PATH, and is BUNDLE set to the Zipper folder?");
            return false;
        }
    }
}
