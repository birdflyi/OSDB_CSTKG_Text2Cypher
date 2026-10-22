# User-triggered API/client/encoding diagnosis

22 October 2026. The user's successful curl example prompted a bounded ten-call diagnosis. No benchmark implementation, question, reference or historical grade was changed. These calls are charged to the same 1,200-attempt limit but excluded from effectiveness scores and recovery predictions.

## Corrected interpretation

Earlier batches recorded real HTTP 500 and read timeouts. Those observations did not establish that the provider still needed server repair. That wording was too strong. The current ten-call check completes successfully, including repeated exact historical failure payloads. The historical failure cause remains unresolved.

The prior 324 saved requests include 75 infrastructure failures. None of these failed message contents contains Han characters; only one contains any non-ASCII character. With the installed requests 2.32.3 serializer, all reconstructed wire bodies are ASCII JSON, are UTF-8 decodable and round-trip exactly. The serializer calls json.dumps then UTF-8 encoding. Prior raw wire packets were not recorded, so this is a source/runtime reconstruction rather than a retrospective packet capture. Chinese input encoding cannot explain the observed failed English requests.

The process environment also configures a local HTTP proxy. Offline session preparation confirms that Authorization is not overwritten. Proxy presence is a potential routing variable, not a demonstrated fault.

| Diagnostic | HTTP 200 with complete content / calls |
|---|---:|
| User-style English request, Python, environment proxy | 1/1 |
| Same English request, Python, explicit direct connection | 1/1 |
| Exact historical full request, low reasoning, environment proxy | 2/2 |
| Same full request, low reasoning, explicit direct connection | 2/2 |
| Same full request with only reasoning changed to high, direct | 1/1 |
| Chinese request as ASCII JSON escapes, direct | 1/1 |
| Identical Chinese request as literal UTF-8 JSON, direct | 1/1 |
| Same English request, curl, explicit direct connection | 1/1 |

The paired full low requests are byte-identical across routes. The Chinese wire encodings differ but decode to identical JSON. All responses report the same system fingerprint; this is reported metadata, not an independent weight verification. Both low and high work in this small check. No result identifies the proxy, reasoning parameter or Chinese encoding as the historical cause. There is no randomized availability estimate or guarantee of future stability.

The diagnostic uses a disclosed 300-second read timeout to distinguish client censoring from returned HTTP errors; the frozen benchmark uses 120 seconds. All ten current responses arrive in 9.359–16.515 seconds, so none needs the longer limit. The check does not explain earlier 120-second timeouts or justify silently changing the benchmark timeout.

## Accounting and next boundary

Ten new calls use 23,274 reported tokens; none has unknown usage. The continuation checkpoint is 334/1200 unique attempts, 75 unknown-usage attempts, zero pending and 1,238,600 known tokens (1,043,295 prompt;195,305 completion). Recorded dispatch sum is 8,341.086 seconds, including prior pacing waits. Currency cost remains unknown. The user's separately supplied curl response is external evidence, not a new agent-ledger call.

Private plan, exact wires, requests and terminals remain in `api_client_transport_diagnostic_v1`. Shareable audit: `E9_API_CLIENT_DIAGNOSTIC.json`. The successful historical diagnostic answers must not replace original benchmark predictions. Further formal work requires a finite admission with frozen methods and task rules, all costs and the existing consecutive-failure stop. A controlled explicit network route is reasonable for reproducibility but must not be called a proven proxy repair.
