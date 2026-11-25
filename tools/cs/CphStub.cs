// A stand-in for the `CPH` object Streamer.bot gives an Execute C# Code sub-action.
// It lets verify-cs.mjs compile the REAL actions/*.cs with the Windows C# compiler and
// run their logic outside Streamer.bot. It implements only the calls Zipper's actions
// make (TryGetArg, GetGlobalVar, SetGlobalVar, LogInfo, LogWarn, WebsocketBroadcastJson),
// and only what mscorlib offers, because that is all Streamer.bot references by default.
//
// This is not Streamer.bot: it proves the C# compiles as C# 5 and that its logic does
// what the checks expect. It does not prove Streamer.bot's own compiler accepts it.
//
// Built from Timbre's tools/cs/CphStub.cs, with one more hook: reading one named
// argument can be made to throw. Ticker Command and Ticker Announce never write a
// global, so the SetGlobalVar hook alone could not reach their catch blocks.

using System;
using System.Collections.Generic;
using System.Globalization;

public class CphStub
{
    public const string DefaultThrowText = "stub: this call was made to fail";

    public Dictionary<string, string> Globals = new Dictionary<string, string>();
    public Dictionary<string, object> Args = new Dictionary<string, object>();
    public List<string> Warnings = new List<string>();
    public List<string> Infos = new List<string>();
    public List<string> Broadcasts = new List<string>();
    public string ThrowOnSetKey = null;  // a global whose write throws
    public string ThrowOnArg = null;     // an argument whose read throws
    public string ThrowText = DefaultThrowText;

    public T GetGlobalVar<T>(string name, bool persisted = true)
    {
        string v;
        if (!Globals.TryGetValue(name, out v)) return default(T);
        return (T)Convert.ChangeType(v, typeof(T), CultureInfo.InvariantCulture);
    }

    public void SetGlobalVar(string name, object value, bool persisted = true)
    {
        if (ThrowOnSetKey != null && name == ThrowOnSetKey) throw new InvalidOperationException(ThrowText);
        Globals[name] = Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    public bool TryGetArg<T>(string name, out T value)
    {
        if (ThrowOnArg != null && name == ThrowOnArg) throw new InvalidOperationException(ThrowText);
        object o;
        if (Args.TryGetValue(name, out o) && o != null)
        {
            value = (T)Convert.ChangeType(o, typeof(T), CultureInfo.InvariantCulture);
            return true;
        }
        value = default(T);
        return false;
    }

    public void LogInfo(string message) { Infos.Add(message); }
    public void LogWarn(string message) { Warnings.Add(message); }
    public void WebsocketBroadcastJson(string json) { Broadcasts.Add(json); }
}
