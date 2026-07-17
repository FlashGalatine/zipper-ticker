// Streamer.bot "Ticker Command" action — the relay from control surfaces to the
// sidecar. SB WebSocket *clients* (the control page, Stream Deck plugins) cannot
// broadcast to other clients directly, so they DoAction this and the C# side
// re-broadcasts { type:"ticker:command", command, value }, which the sidecar
// consumes off General.Custom.
//
// Call styles:
//   • control.html — DoAction args { command:"setUrl", value:"https://..." }
//   • Stream Deck  — Set Argument sub-actions before this one, same two args
//   • Chat Command triggers — attach them directly, NO Set Argument needed.
//     SB command triggers set their own `command` (the matched chat command,
//     e.g. "!ticker") + `commandId`; this action detects that and maps:
//       "!ticker <cmd> [value…]" → first word of rawInput = command, rest = value
//     Any other chat command "!foo" relays as command "foo" with rawInput as value.
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

            string c, v, raw, cid;
            CPH.TryGetArg("rawInput", out raw);
            raw = raw == null ? "" : raw.Trim();

            // A chat Command trigger sets ITS OWN `command` argument (the matched
            // chat command, e.g. "!ticker") plus a `commandId` — which a control-page
            // DoAction never carries. Without this branch the trigger's command
            // shadows ours and the sidecar sees unknown "!ticker".
            if (CPH.TryGetArg("commandId", out cid) && !string.IsNullOrWhiteSpace(cid))
            {
                CPH.TryGetArg("command", out c);
                string chatCmd = (c ?? "").Trim().TrimStart('!').ToLowerInvariant();
                if (chatCmd == "ticker")
                {
                    // "!ticker setUrl https://..." → command=setUrl, value=the rest
                    int sp = raw.IndexOf(' ');
                    command = sp < 0 ? raw : raw.Substring(0, sp);
                    value = sp < 0 ? "" : raw.Substring(sp + 1).Trim();
                }
                else
                {
                    // A dedicated chat command (e.g. "!pollnow"): the chat command IS
                    // the sidecar command; anything typed after it rides as value.
                    command = chatCmd;
                    value = raw;
                }
            }
            else if (CPH.TryGetArg("command", out c) && !string.IsNullOrWhiteSpace(c))
            {
                // Control page / Stream Deck: explicit { command, value } args.
                command = c.Trim();
                if (CPH.TryGetArg("value", out v) && v != null) value = v;
            }
            else if (raw.Length > 0)
            {
                // Bare rawInput fallback (manual Set Argument setups).
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
