// Streamer.bot "Ticker Announce" action — interrupts the ticker strip with a
// custom announcement (the overlay pauses the results crawl, shows the
// announcement for `duration` seconds, then resumes). Announcements are
// ephemeral: they are broadcast but never persisted, so reconnecting overlays
// only replay tournament data.
//
// Broadcasts: { type:"ticker:announce", kind, text, duration }
//   kind ∈ sub | raid | milestone | custom   (anything else renders as custom)
//
// Call styles:
//   • SB Twitch triggers — add this action to your Subscription / Gift Sub /
//     Raid triggers. With no `text` argument it auto-composes one from the
//     trigger's own arguments (%user%, %viewers%, …):
//       raid  → "UserName is raiding with 25 viewers!"
//       sub   → "UserName just subscribed!"
//     Pass an explicit `text` argument (Core → Arguments → Set Argument before
//     this sub-action) to override, e.g. "%user% gifted %giftCount% subs!".
//   • Control page / Stream Deck — DoAction with args { kind, text, duration }.
//   • Sidecar milestones — the Zipper sidecar calls this with kind "milestone"
//     when a StreamElements counter crosses a configured step (README.md).
//
// Setup: add an action named EXACTLY "Ticker Announce" with a single
// Core -> C# -> Execute C# Code sub-action holding everything below (COMPILE
// must succeed). Default reference set only — JSON is hand-written.

using System;
using System.Text;
using System.Globalization;

public class CPHInline
{
    public bool Execute()
    {
        try
        {
            string kind = Arg("kind");
            string text = Arg("text");
            string user = FirstArg("user", "userName", "displayName");

            // Auto-compose from common Twitch trigger arguments when no explicit
            // text was set. Raids carry a viewer count; subs carry the user.
            if (text.Length == 0)
            {
                string viewers = FirstArg("viewers", "viewerCount", "raiders");
                if (viewers.Length > 0)
                {
                    if (kind.Length == 0) kind = "raid";
                    text = (user.Length > 0 ? user : "Someone") + " is raiding with " + viewers + " viewers!";
                }
                else if (user.Length > 0)
                {
                    if (kind.Length == 0) kind = "sub";
                    text = user + " just subscribed!";
                }
            }
            if (kind.Length == 0) kind = "custom";
            if (text.Length == 0)
            {
                CPH.LogWarn("[Ticker Announce] no text and nothing to compose from — skipped");
                return false;
            }

            int duration;
            string durRaw = Arg("duration");
            if (!int.TryParse(durRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out duration)) duration = 8;
            duration = Math.Min(30, Math.Max(2, duration));

            string json = "{\"type\":\"ticker:announce\",\"kind\":" + JsonStr(kind)
                + ",\"text\":" + JsonStr(text)
                + ",\"duration\":" + duration.ToString(CultureInfo.InvariantCulture) + "}";
            CPH.LogInfo("[Ticker Announce] " + json);
            CPH.WebsocketBroadcastJson(json);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogWarn("[Ticker Announce] ERROR: " + ex);
            CPH.WebsocketBroadcastJson("{\"type\":\"ticker:error\",\"message\":" + JsonStr(ex.Message) + "}");
            return false;
        }
    }

    string Arg(string key)
    {
        string v;
        return CPH.TryGetArg(key, out v) && v != null ? v.Trim() : "";
    }

    string FirstArg(params string[] keys)
    {
        foreach (string k in keys)
        {
            string v = Arg(k);
            if (v.Length > 0) return v;
        }
        return "";
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
