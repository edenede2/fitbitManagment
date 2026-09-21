# Google Health scope justification — study 385/23

This is the proposed verification justification. It must stay consistent with the
final ethics-approved addendum, the OAuth consent screen, and the features shown in
the reviewer video. All requested access is read-only.

| Requested scope | Data used | Participant-visible/reviewer feature | Research necessity |
| --- | --- | --- | --- |
| `googlehealth.activity_and_fitness.readonly` | Steps and physical activity | Activity/steps status and the corresponding bounded raw archive | Measures physical activity needed for the approved research and supports data-quality checks. |
| `googlehealth.health_metrics_and_measurements.readonly` | Heart rate and respiratory rate | Physiology status and the corresponding bounded raw archive | Supplies the physiological measures named in the approved addendum. No write or clinical-decision feature exists. |
| `googlehealth.sleep.readonly` | Sleep sessions, stages/intervals, duration, and timestamps | Sleep status and monthly sleep archive | Sleep patterns are an explicit variable in the approved study question. |
| `googlehealth.settings.readonly` | Assigned watch model, battery level/status, and last synchronization time | Device Details status shown to authorized study staff | Identifies charging or synchronization problems and supports data-completeness checks. The app does not modify settings. |

These four scopes are the narrowest available read-only permissions that expose the
approved health categories and technical device information. The implementation does
not request write access, mindfulness data, or unrelated Google account data.
Provider availability determines which approved data types are present.

Staff identity is handled by a separate web OAuth client using only `openid`,
`profile`, and `email`. Participant health scopes must not be added to the staff
client.
