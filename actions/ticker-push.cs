// Streamer.bot "Ticker Push" action — the one way a ticker:update reaches the pages.
//
// Dual mode:
//   • Sidecar path — zipper-sidecar.mjs calls DoAction "Ticker Push" with a
//     `tickerPayload` argument holding the full, pre-serialized ticker:update
//     JSON. The action persists it to the global `ticker.payload` and broadcasts
//     it verbatim.
//   • Sync-on-connect path — panel-core.js calls DoAction "Ticker Push" with NO
//     tickerPayload when an overlay (re)connects. The action re-broadcasts the
//     persisted global (or an empty ticker:update if nothing was ever pushed),
//     recreating the state-on-connect replay Streamer.bot doesn't have.
//
// HOW TO USE (see README.md for the full walkthrough):
//   1. Streamer.bot -> Actions -> add an action named EXACTLY "Ticker Push"
//      (the name matters: both the sidecar and panel-core call it by name).
//   2. Add a sub-action: Core -> C# -> Execute C# Code. Paste EVERYTHING below and
//      click COMPILE — it must report success. A compile error means the action
//      runs but broadcasts nothing, which shows up as a blank/stale ticker.
//
// QUEUE: put this action, "Ticker Command" and "Ticker Announce" in ONE BLOCKING queue
// (README, setup step 1). In the Default queue, runs that overlap mix their arguments.
//
// NOTE: uses ONLY types in Streamer.bot's default C# reference set — JSON is
// hand-written (no Newtonsoft). Verified pattern from Tally on Streamer.bot 1.0.4.
//
// ERRORS, so failures are visible, not silent. A tickerPayload that does not start
// with { and end with } is refused: nothing is stored, one warning is logged, and
// { type:"ticker:error", code:"bad-payload", message:"tickerPayload is not a JSON object", action:"Ticker Push" }
// is broadcast. An exception is logged in full and broadcast as
// { type:"ticker:error", code:"exception", message:"<fixed text>", action:"Ticker Push" }: the exception's
// own message goes to Streamer.bot's log only, never to the pages. action names the
// sender (ACTION_NAME); the control page and the sidecar do not read it.
//
// mock-sb-server.mjs (runTickerPush) copies Execute() line for line, and
// tools/cs/ticker-cases.json is the table both are run against. Change them together.

using System;
using System.Text;

public class CPHInline
{
    const string EMPTY_UPDATE = "{\"type\":\"ticker:update\",\"tournament\":null,\"matches\":[],\"standings\":[],\"caps\":{\"text\":\"\",\"logo\":\"\"}}";
    const string BAD_PAYLOAD_CODE = "bad-payload";
    const string BAD_PAYLOAD_MESSAGE = "tickerPayload is not a JSON object";
    const string EXCEPTION_MESSAGE = "Ticker Push threw an exception";
    // This action's exact name, which every ticker:error it sends carries as action.
    const string ACTION_NAME = "Ticker Push";

    public bool Execute()
    {
        try
        {
            string payload;
            if (CPH.TryGetArg("tickerPayload", out payload) && !string.IsNullOrWhiteSpace(payload))
            {
                // Sidecar push: cheap sanity check only — the sidecar owns the shape.
                payload = payload.Trim();
                if (!payload.StartsWith("{") || !payload.EndsWith("}"))
                {
                    // Refused: the stored payload stays as it was. The log gives the length,
                    // not the text, which is not ours to quote.
                    CPH.LogWarn("[Ticker Push] refused (" + BAD_PAYLOAD_CODE + "): " + BAD_PAYLOAD_MESSAGE
                        + " (" + payload.Length + " chars); nothing was stored");
                    CPH.WebsocketBroadcastJson(ErrorJson(BAD_PAYLOAD_CODE, BAD_PAYLOAD_MESSAGE));
                    return false;
                }
                CPH.SetGlobalVar("ticker.payload", payload, true);
            }
            else
            {
                // Overlay connect-sync: replay the persisted payload.
                payload = CPH.GetGlobalVar<string>("ticker.payload", true);
                if (string.IsNullOrWhiteSpace(payload)) payload = EMPTY_UPDATE;
            }

            CPH.LogInfo("[Ticker Push] broadcasting " + payload.Length + " bytes");
            CPH.WebsocketBroadcastJson(payload);
            return true;
        }
        catch (Exception ex)
        {
            // The exception goes to the log. The pages get a code and a fixed text.
            CPH.LogWarn("[Ticker Push] ERROR: " + ex);
            try { CPH.WebsocketBroadcastJson(ErrorJson("exception", EXCEPTION_MESSAGE)); }
            catch (Exception) { }
            return false;
        }
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
