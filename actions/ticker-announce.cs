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
//   • SB Twitch triggers — add this action to your Subscription, Resubscription,
//     Gift Subscription, Gift Bomb and Raid triggers. With no `text` argument it
//     composes one from the trigger's own arguments:
//       raid      → "UserName is raiding with 25 viewers!"   (user, viewers)
//       sub       → "UserName just subscribed!"               (user; a resub too)
//       gift sub  → "Gifter gifted a sub to Recipient!"       (user, recipientUser)
//       gift bomb → "Gifter gifted 5 subs!", or "Gifter gifted a sub!" for one
//                                                             (user, gifts)
//     A missing name reads "Someone", and so does an anonymous gift (its anonymous
//     argument is true), whatever `user` holds. A Gift Subscription run that is part of a
//     gift bomb (its fromGiftBomb argument is true) announces nothing, whatever its other
//     arguments: the Gift Bomb run announces the whole bomb once. So add the Gift Bomb
//     trigger with the Gift Subscription one, or a bomb is not announced at all, and add
//     them only once this action is in the blocking queue (QUEUE below): in the Default
//     queue a bomb's runs, which come together, can mix their arguments, and the Gift
//     Bomb run can then read a gift sub's fromGiftBomb and be skipped too.
//     The arguments are the ones Test Trigger gives on Streamer.bot 1.0.7 (its Gift
//     Subscription test gave fromGiftBomb true, so that test is skipped). NOT checked
//     against a live gift bomb: whether Gift Subscription fires once per recipient
//     during one, and in what order (the skip does not depend on the order); what
//     `user` holds for an anonymous gift; and whether a live Gift Bomb run names its
//     recipients (not used here).
//     Pass an explicit `text` argument (Core → Arguments → Set Argument before
//     this sub-action) to override, e.g. "%user% gifted %gifts% subs!".
//   • Control page / Stream Deck — DoAction with args { kind, text, duration }.
//   • Sidecar milestones — the Zipper sidecar calls this with kind "milestone"
//     when a StreamElements counter crosses a configured step (README.md).
//
// QUEUE: put this action, "Ticker Command" and "Ticker Push" in ONE BLOCKING queue
// (README, setup step 1). In the Default queue, runs that overlap mix their arguments.
//
// ERRORS: an exception is logged here in full, and broadcast as
// { type:"ticker:error", code:"exception", message:"<fixed text>", action:"Ticker Announce" }:
// the exception's own message goes to Streamer.bot's log only, never to the pages. action
// names the sender (ACTION_NAME); the control page and the sidecar do not read it.
//
// Setup: add an action named EXACTLY "Ticker Announce" with a single
// Core -> C# -> Execute C# Code sub-action holding everything below (COMPILE
// must succeed). Default reference set only — JSON is hand-written.
//
// mock-sb-server.mjs (runTickerAnnounce) copies Execute() line for line, and
// tools/cs/ticker-cases.json is the table both are run against. Change them together.

using System;
using System.Text;
using System.Globalization;

public class CPHInline
{
    const string EXCEPTION_MESSAGE = "Ticker Announce threw an exception";
    // This action's exact name, which every ticker:error it sends carries as action.
    const string ACTION_NAME = "Ticker Announce";

    public bool Execute()
    {
        try
        {
            // A Gift Subscription run that is part of a gift bomb: the Gift Bomb run
            // announces the bomb once, so this one is skipped. Without this, a bomb of 20
            // would queue 20 announcements, one per recipient.
            if (Arg("fromGiftBomb").ToLowerInvariant() == "true")
            {
                CPH.LogInfo("[Ticker Announce] a gift sub from a gift bomb: skipped (the Gift Bomb run announces the bomb)");
                return true;
            }

            string kind = Arg("kind");
            string text = Arg("text");
            string user = FirstArg("user", "userName", "displayName");

            // Auto-compose from common Twitch trigger arguments when no explicit
            // text was set. Raids carry a viewer count, a gift bomb its number of gifts,
            // a gift sub its recipient; subs carry the user.
            if (text.Length == 0)
            {
                // An anonymous gift (its anonymous argument is true) reads "Someone" whatever
                // `user` holds: what Streamer.bot puts there for one was not seen.
                bool anonymous = Arg("anonymous").ToLowerInvariant() == "true";
                string who = user.Length > 0 && !anonymous ? user : "Someone";
                string viewers = FirstArg("viewers", "viewerCount", "raiders");
                string gifts = Arg("gifts");
                string recipient = Arg("recipientUser");
                if (viewers.Length > 0)
                {
                    if (kind.Length == 0) kind = "raid";
                    text = who + " is raiding with " + viewers + " viewers!";
                }
                else if (gifts.Length > 0)
                {
                    if (kind.Length == 0) kind = "sub";
                    text = gifts == "1" ? who + " gifted a sub!" : who + " gifted " + gifts + " subs!";
                }
                else if (recipient.Length > 0)
                {
                    if (kind.Length == 0) kind = "sub";
                    text = who + " gifted a sub to " + recipient + "!";
                }
                else if (user.Length > 0)
                {
                    if (kind.Length == 0) kind = "sub";
                    text = who + " just subscribed!";
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
            // The exception goes to the log. The pages get a code and a fixed text.
            CPH.LogWarn("[Ticker Announce] ERROR: " + ex);
            try { CPH.WebsocketBroadcastJson(ErrorJson("exception", EXCEPTION_MESSAGE)); }
            catch (Exception) { }
            return false;
        }
    }

    // An argument as text, trimmed; "" when it is missing. It is read as an object and
    // turned into text here, so a trigger's Boolean or Int32 (fromGiftBomb, gifts,
    // viewers) reads as "True" or "5" whatever Streamer.bot's TryGetArg does when asked
    // for a string.
    string Arg(string key)
    {
        object v;
        return CPH.TryGetArg(key, out v) && v != null ? Convert.ToString(v, CultureInfo.InvariantCulture).Trim() : "";
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
