# Google Health scope justification — study 385/23

This is the proposed verification justification. It must stay consistent with the
final ethics-approved addendum, the OAuth consent screen, and the features shown in
the reviewer video. All requested access is read-only.

| Requested scope | Data used | Participant-visible/reviewer feature | Research necessity |
| --- | --- | --- | --- |
| `googlehealth.activity_and_fitness.readonly` | Steps, activity intervals, and calories | Activity/steps status and the corresponding bounded raw archive | Measures daily physical activity and supports completeness checks needed for the approved relationship between sleep, experience, and adult responses. |
| `googlehealth.health_metrics_and_measurements.readonly` | Heart rate, daily heart-rate variability, sleep-derived skin temperature, and respiratory rate | Physiology status and daily/monthly archive artifacts | Supplies the approved physiological measures and operational completeness checks. No write or clinical-decision feature exists. |
| `googlehealth.sleep.readonly` | Sleep sessions, stages/intervals, duration, and timestamps | Sleep status and monthly sleep archive | Sleep patterns are an explicit variable in the approved study question. |

The implementation does not request write access. Provider availability determines
which listed data types are present. Missing or unsupported types are recorded as an
operational condition and do not cause a broader scope request.

Staff identity is handled by a separate web OAuth client using only `openid`,
`profile`, and `email`. Participant health scopes must not be added to the staff
client.
