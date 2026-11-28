// Runs a copy of Ticker Sidecar Start (compiled in beside this file as class CPHInline)
// once and writes what happened. Used only by verify-cs.mjs, which makes the copy: its
// BUNDLE and GUARD_PORT read Bundle and Port below, so the copy probes a port of the
// suite's own and launches only what the suite put in its own folder. The real action,
// with 7496, is compiled and never run.
//
//   ticker-sidecar-start-run.exe <port> <bundle folder> <out.txt>
//
// Output, one line, TAB separated:
//   ret=<true|false>  ms=<how long Execute() took>  warn=<warnings>  info=<info lines>
//   threw=<the exception's type>, only when one escaped Execute()
//   warns=<the warnings>  infos=<the info lines>  (each joined by U+001E; line breaks and
//   tabs as spaces)

using System;
using System.Diagnostics;
using System.IO;
using System.Text;

public static class LauncherHarness
{
    public static int Port;
    public static string Bundle;

    static string Lines(System.Collections.Generic.List<string> list)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < list.Count; i++)
        {
            if (i > 0) sb.Append('\u001e');
            sb.Append(list[i].Replace('\r', ' ').Replace('\n', ' ').Replace('\t', ' '));
        }
        return sb.ToString();
    }

    public static int Main(string[] argv)
    {
        Port = int.Parse(argv[0]);
        Bundle = argv[1];
        var cph = new CphStub();
        var action = new CPHInline();
        action.CPH = cph;
        bool ret = false;
        string threw = null;
        var sw = Stopwatch.StartNew();
        try { ret = action.Execute(); }
        catch (Exception ex) { threw = ex.GetType().FullName; }
        long ms = sw.ElapsedMilliseconds;
        var outp = new StringBuilder();
        outp.Append("ret=").Append(ret ? "true" : "false");
        outp.Append("\tms=").Append(ms);
        outp.Append("\twarn=").Append(cph.Warnings.Count);
        outp.Append("\tinfo=").Append(cph.Infos.Count);
        if (threw != null) outp.Append("\tthrew=").Append(threw);
        outp.Append("\twarns=").Append(Lines(cph.Warnings));
        outp.Append("\tinfos=").Append(Lines(cph.Infos));
        File.WriteAllText(argv[2], outp.ToString() + "\n", new UTF8Encoding(false));
        return 0;
    }
}
