# Housekeeping & retention

The outbox is not an archive: a message should only stay in the `deferred_message` table temporarily.
To support troubleshooting and analysis, successfully sent messages are not deleted immediately —
they are kept for a configurable retention period and removed by a scheduled housekeeping job.

## How it works

`OutboxHouseKeepingScheduler` runs `OutboxHouseKeeping.deleteOldMessages()` on the cron schedule
`house-keeping-schedule` (default `0 0 3 * * *`, i.e. daily at 03:00), guarded by a ShedLock lock
(`outbox-message-house-keeping-tasks`) so it runs on a single instance. Each run deletes:

- **sent** messages whose send timestamp is older than `sent-message-retention-duration`, and
- **unsent** messages whose `created` timestamp is older than `unsent-message-retention-duration`.

Deletion is paged: each run processes up to `house-keeping-max-pages` pages of `house-keeping-page-size`
rows, each page in its own transaction (forcing a Hibernate flush per page, keeping transactions small
and minimizing lock duration). The maximum number of messages deleted per kind in one run is therefore
`house-keeping-page-size * house-keeping-max-pages`.

## Retention configuration

| Property                            | Default | Applies to                                          |
|-------------------------------------|---------|-----------------------------------------------------|
| `sent-message-retention-duration`   | `P2D`   | Successfully sent messages                           |
| `unsent-message-retention-duration` | `P30D`  | Messages that were not (yet) successfully sent       |
| `house-keeping-schedule`            | `0 0 3 * * *` | Cron expression for when the cleanup runs      |
| `house-keeping-page-size`           | `500`   | Page size of the delete queries                      |
| `house-keeping-max-pages`           | `100000`| Max pages per run (caps how much one run can delete) |

Note: a longer `unsent-message-retention-duration` keeps [failed](failure-handling.md) messages around
long enough to be investigated and resent before they are purged.

## Related

- [Failure handling](failure-handling.md)
- [Database schema & migrations](database.md)
- [Configuration reference](configuration.md)
- [jeap-messaging-outbox](../README.md)
