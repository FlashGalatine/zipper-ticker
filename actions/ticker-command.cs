// Streamer.bot "Ticker Command" action — the relay from control surfaces to the
// sidecar. SB WebSocket *clients* (the control page, Stream Deck plugins) cannot
// broadcast to other clients directly, so they DoAction this and the C# side
// re-broadcasts { type:"ticker:command", command, value }, which the sidecar
// consumes off General.Custom.
//
// Call styles:
//   • control.html — DoAction args { command:"setUrl", value:"https://..." }
//   • Stream Deck  — a button that sets the same two args (command, value) and runs
//     Ticker Command, or an action of its own that does (Set Argument sub-actions,
//     then Core → Actions → Run Action; how it carries the arguments was not checked
//     here). Never add Set Argument sub-actions to Ticker Command itself:
//     they would run on every run and replace the command and value of every caller.
//     These two (an explicit `command` argument, no `commandId`) are relayed as
//     they are, whatever the command.
//   • Chat Command triggers — attach them directly, NO Set Argument needed.
//     SB command triggers set their own `command` (the matched chat command,
//     e.g. "!ticker") + `commandId`; this action detects that and maps:
//       "!ticker <cmd> [value…]" → first word of rawInput = command, rest = value
//     Any other chat command "!foo" is the command "foo", with rawInput as value.
//     Chat input, through "!ticker <cmd>" or a dedicated "!cmd", relays ONLY the
//     commands in CHAT_ALLOWED below: start, stop, pollNow, status and setTopN,
//     matched without regard to case and relayed as written there ("!ticker
//     settopn 4" relays setTopN, "!pollnow" relays pollNow). Anything else, __status
//     included, is refused with one warning in Streamer.bot's log, and nothing is
//     relayed or broadcast. The same holds for a bare `rawInput` with no `command`
//     argument (a trigger or a Set Argument setup that carries only rawInput).
//     Everything else stays on the control page and a Stream Deck.
//
// Commands the sidecar understands (docs/PROTOCOL.md): setUrl, start, stop,
// pollNow, setInterval, setCaps, setCapsText, setCapsLogo, setOptions,
// setMaxItems, setTopN, setMessage, setMessageText, setMessageEnabled,
// setAutoSchedule, status. Names match without regard to case. "__status" is
// reserved: the sidecar sends its status through this action that way.
//
// LOG: each relay is logged at info, except a `status` or `__status` that does not
// come from chat (the control page asks for a status every 10 seconds, and the sidecar
// sends one after every command and poll): those are relayed and not logged. A chat
// command, "!ticker status" included, is logged; a refused one is a warning.
//
// QUEUE: put this action, "Ticker Push" and "Ticker Announce" in ONE BLOCKING queue
// (README, setup step 1). In the Default queue, runs that overlap mix their arguments.
//
// ERRORS: an exception is logged here in full, and broadcast as
// { type:"ticker:error", code:"exception", message:"<fixed text>", action:"Ticker Command" }:
// the exception's own message goes to Streamer.bot's log only, never to the pages. action
// names the sender (ACTION_NAME); the control page and the sidecar do not read it.
//
// Setup: add an action named EXACTLY "Ticker Command" with a single
// Core -> C# -> Execute C# Code sub-action holding everything below (COMPILE must
// report success). Default reference set only — JSON is hand-written.
//
// mock-sb-server.mjs (runTickerCommand) copies Execute()'s relay, its chat parsing and
// the chat list (what is refused), but not its log lines (which relays are logged at
// info, and the text a refused command's warning quotes) or its catch block (the mock's
// /mock/throw switch stands in for a throw). verify-cs.mjs (npm run verify:cs, not part
// of npm run verify) checks those against the compiled action.
// tools/cs/ticker-cases.json is the table both are run against. Change them together.

using System;
using System.Text;

public class CPHInline
{
    // What chat may send (decision 1). Matched without regard to case, relayed as written here.
    static readonly string[] CHAT_ALLOWED = { "start", "stop", "pollNow", "status", "setTopN" };
    // How many characters of a refused chat command its warning quotes.
    const int REFUSED_TEXT_MAX = 80;
    const string EXCEPTION_MESSAGE = "Ticker Command threw an exception";
    // This action's exact name, which every ticker:error it sends carries as action.
    const string ACTION_NAME = "Ticker Command";

    public bool Execute()
    {
        try
        {
            string command = "";
            string value = "";
            bool chat = false;      // a chat Command trigger (commandId set)
            bool bareInput = false; // rawInput alone, with no command argument

            string c, v, raw, cid;
            CPH.TryGetArg("rawInput", out raw);
            raw = raw == null ? "" : raw.Trim();

            // A chat Command trigger sets ITS OWN `command` argument (the matched chat
            // command, e.g. "!ticker") plus a `commandId` — which a control-page DoAction
            // never carries. Without this branch the trigger's command shadows ours and the
            // sidecar sees unknown "!ticker".
            if (CPH.TryGetArg("commandId", out cid) && !string.IsNullOrWhiteSpace(cid))
            {
                chat = true;
                CPH.TryGetArg("command", out c);
                string chatCmd = (c ?? "").Trim().TrimStart('!').ToLowerInvariant();
                if (chatCmd == "ticker")
                {
                    // "!ticker setTopN 4" → command=setTopN, value=the rest
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
                // Bare rawInput (a trigger, or a Set Argument setup, that carries only
                // rawInput): limited to the chat list below, as chat is.
                bareInput = true;
                int sp = raw.IndexOf(' ');
                command = sp < 0 ? raw : raw.Substring(0, sp);
                value = sp < 0 ? "" : raw.Substring(sp + 1).Trim();
            }

            if (string.IsNullOrWhiteSpace(command))
            {
                CPH.LogWarn("[Ticker Command] no command argument — nothing to relay");
                return false;
            }

            // Chat, and a bare rawInput, may send only the allowed commands (decision 1).
            // A refusal is never broadcast: the control page shows ticker:error, so a
            // broadcast refusal would let chat write the dock's error line.
            if (chat || bareInput)
            {
                string allowed = ChatName(command);
                if (allowed == null)
                {
                    string sent = Clip(value.Length > 0 ? command + " " + value : command);
                    if (chat)
                        CPH.LogWarn("[Ticker Command] refused from chat: \"" + sent
                            + "\"; chat may send only start, stop, pollNow, status and setTopN, so nothing was relayed");
                    else
                        CPH.LogWarn("[Ticker Command] refused from a rawInput with no command argument: \"" + sent
                            + "\"; that way, as from chat, only start, stop, pollNow, status and setTopN are relayed, so nothing was."
                            + " A Stream Deck or a Set Argument setup should set the command and value arguments instead");
                    return false;
                }
                command = allowed;
            }

            string json = "{\"type\":\"ticker:command\",\"command\":" + JsonStr(command)
                + ",\"value\":" + JsonStr(value) + "}";
            // A status the control page or the sidecar asks for is not logged: one page asks
            // every 10 seconds, and the sidecar sends one after every command and poll.
            if (chat || !IsStatus(command)) CPH.LogInfo("[Ticker Command] " + json);
            CPH.WebsocketBroadcastJson(json);
            return true;
        }
        catch (Exception ex)
        {
            // The exception goes to the log. The pages get a code and a fixed text.
            CPH.LogWarn("[Ticker Command] ERROR: " + ex);
            try { CPH.WebsocketBroadcastJson(ErrorJson("exception", EXCEPTION_MESSAGE)); }
            catch (Exception) { }
            return false;
        }
    }

    // The allowed name a chat command stands for, as CHAT_ALLOWED writes it, or null.
    static string ChatName(string command)
    {
        string lower = command.ToLowerInvariant();
        foreach (string name in CHAT_ALLOWED)
            if (lower == name.ToLowerInvariant()) return name;
        return null;
    }

    static bool IsStatus(string command)
    {
        string lower = command.ToLowerInvariant();
        return lower == "status" || lower == "__status";
    }

    // A refused chat command as its warning quotes it: control characters and line
    // separators as spaces, so it stays on one line of the log, and cut to
    // REFUSED_TEXT_MAX characters (never through an emoji) with an ellipsis.
    static string Clip(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (char ch in s)
            sb.Append(char.IsControl(ch) || (int)ch == 0x2028 || (int)ch == 0x2029 ? ' ' : ch);
        string t = sb.ToString();
        if (t.Length <= REFUSED_TEXT_MAX) return t;
        int cut = REFUSED_TEXT_MAX;
        if (char.IsHighSurrogate(t[cut - 1])) cut--;
        return t.Substring(0, cut) + (char)0x2026;
    }

    // { type, code, message, action }, in that order; action is this action's name.
    static string ErrorJson(string code, string message)
    {
        return "{\"type\":\"ticker:error\",\"code\":" + JsonStr(code) + ",\"message\":" + JsonStr(message)
            + ",\"action\":" + JsonStr(ACTION_NAME) + "}";
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
