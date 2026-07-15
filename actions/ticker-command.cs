// Streamer.bot "Ticker Command" action — the relay from control surfaces to the
// sidecar. SB WebSocket *clients* (the control page, Stream Deck plugins) cannot
// broadcast to other clients directly, so they DoAction this and the C# side
// re-broadcasts { type:"ticker:command", command, value }, which the sidecar
// consumes off General.Custom.
//
// Call styles:
//   • control.html — DoAction args { command:"setUrl", value:"https://..." }
//   • Stream Deck  — Set Argument sub-actions before this one, same two args
//   • Chat trigger — "!ticker <cmd> [value...]" with the trigger's rawInput arg:
//     the first word becomes command, the rest value (e.g. "!ticker pollNow").
//
// Commands the sidecar understands (docs/PROTOCOL.md): setUrl, start, stop,
// pollNow, setInterval, setCapsText, setCapsLogo, setMaxItems, setMessageText,
// setMessageEnabled, status.
//
// Setup: add an action named EXACTLY "Ticker Command" with a single
// Core -> C# -> Execute C# Code sub-action holding everything below (COMPILE must
// report success). Default reference set only — JSON is hand-written.

using System;
using System.Text;

public class CPHInline
{
    public bool Execute()
    {
        try
        {
            string command = "";
            string value = "";

            string c, v, raw;
            if (CPH.TryGetArg("command", out c) && !string.IsNullOrWhiteSpace(c))
            {
                command = c.Trim();
                if (CPH.TryGetArg("value", out v) && v != null) value = v;
            }
            else if (CPH.TryGetArg("rawInput", out raw) && !string.IsNullOrWhiteSpace(raw))
            {
                // Chat style: "!ticker setUrl https://..." → rawInput = "setUrl https://..."
                raw = raw.Trim();
                int sp = raw.IndexOf(' ');
                command = sp < 0 ? raw : raw.Substring(0, sp);
                value = sp < 0 ? "" : raw.Substring(sp + 1).Trim();
            }

            if (string.IsNullOrWhiteSpace(command))
            {
                CPH.LogWarn("[Ticker Command] no command argument — nothing to relay");
                return false;
            }

            string json = "{\"type\":\"ticker:command\",\"command\":" + JsonStr(command)
                + ",\"value\":" + JsonStr(value) + "}";
            CPH.LogInfo("[Ticker Command] " + json);
            CPH.WebsocketBroadcastJson(json);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogWarn("[Ticker Command] ERROR: " + ex);
            CPH.WebsocketBroadcastJson("{\"type\":\"ticker:error\",\"message\":" + JsonStr(ex.Message) + "}");
            return false;
        }
    }

    // Minimal, correct JSON string encoder (escapes quotes, backslashes, controls).
    static string JsonStr(string s)
    {
        if (s == null) return "\"\"";
        var sb = new StringBuilder(s.Length + 2);
        sb.Append('"');
        foreach (char c in s)
        {
            switch (c)
            {
                case '"':  sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < ' ') sb.Append("\\u").Append(((int)c).ToString("x4"));
                    else sb.Append(c);
                    break;
            }
        }
        sb.Append('"');
        return sb.ToString();
    }
}
