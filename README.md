# Booky2.0

Booky2.0 is a Discord slash-command bot that searches Bookshelf for audiobooks, adds a selected edition, and asks Bookshelf/Prowlarr to find a download.

## Container image

The included `Dockerfile` creates a production Python 3.12 image that:

- runs as an unprivileged user;
- installs only the bot's Python dependencies;
- uses unbuffered logs suitable for container log collection;
- handles shutdown through `SIGTERM`;
- exposes an HTTP health service on `PORT` (default `8080`);
- includes an image-level health check at `/healthz`.

The GitHub Actions workflow at `.github/workflows/publish-container.yml` automatically builds and publishes a Bunny-compatible `linux/amd64` image to GitHub Container Registry whenever the container files change. It also supports manual runs from the repository's **Actions** tab.

The published image reference is:

```text
ghcr.io/<github-owner>/<repository>:latest
```

The `latest` tag is published from the repository's default branch. Branch and commit-specific tags are also retained. Do not include credentials in the image; configure them as container environment variables.

### Make the GHCR package visible to Bunny

After the first successful **Publish Booky2.0 container** workflow run:

1. Open the repository or organization **Packages** page on GitHub.
2. Select the newly created container package.
3. Open **Package settings** and change visibility to **Public** for the simplest Bunny setup.
4. If the package must remain private, connect GHCR under Bunny **Magic Containers → Image Registries** using GitHub credentials with package-read access.
5. In Bunny, select or enter `ghcr.io/<github-owner>/<repository>:latest` and refresh the image list.

## Required environment variables

| Variable | Required | Description |
| --- | --- | --- |
| `DISCORD_TOKEN` | Yes | Discord bot token. `DISCORD_BOT_TOKEN` is also accepted for compatibility. |
| `BOOKSHELF_URL` | Yes | Base URL Bunny can use to reach Bookshelf, without a trailing slash. |
| `BOOKSHELF_API_KEY` | Yes | Bookshelf/Readarr API key. Store it as a secret. |
| `ABS_REPORT_CHANNEL_ID` | For `/absreport` | Discord channel ID that receives public audiobook issue reports. |
| `JACKETT_URL` | For fallback | Base URL Bunny can use to reach Jackett, without a trailing slash. |
| `JACKETT_API_KEY` | For fallback | Jackett API key. Store it as a secret. |
| `DOWNLOAD_POLL_SECONDS` | No | Seconds between completion checks. Defaults to `30`, minimum `10`. |
| `DOWNLOAD_WATCH_SECONDS` | No | Maximum monitoring time. Defaults to `86400` (24 hours), minimum `300`. |
| `PORT` | No | Health server port. Defaults to `8080`. |
| `LOG_LEVEL` | No | Python log level. Defaults to `INFO`. |

The old default `http://bookshelf:8787` works only when that hostname is resolvable from the container. For a Bunny-hosted bot, set `BOOKSHELF_URL` to an address reachable from the Magic Containers network.

After Prowlarr grabs a release, Booky2.0 monitors the selected Bookshelf book. When Bookshelf reports a downloaded file, the bot mentions the requesting user in the original Discord channel and confirms that the title is available for playback. The container must remain running while the download is monitored.

When both Jackett variables are configured, Booky2.0 searches Jackett's enabled indexers after Prowlarr finds no download. It shows up to five safe release links in Discord but does not automatically download them. Links containing API keys, passkeys, or authentication tokens are omitted. If Jackett runs only on a home network, Bunny cannot reach it unless you expose it through a secure authenticated tunnel or private network; do not expose an unauthenticated Jackett instance directly to the internet.

## Bunny Magic Containers setup

1. Confirm the GitHub publishing workflow completed successfully and the GHCR package is public or connected to Bunny.
2. Create a Magic Container from `ghcr.io/<github-owner>/<repository>:latest`.
3. Configure container port `8080`, or use the same custom value for both `PORT` and the port settings below.
4. Add the required environment variables under **Container Settings → Edit → Environment Variables**.
5. Keep the service at exactly **one replica**. Multiple replicas using the same Discord token can process the same bot workload and should not be used for autoscaling.
6. Configure monitoring under **Container Settings → Monitoring**:

| Probe | Type | Port | Path | Purpose |
| --- | --- | --- | --- | --- |
| Startup | HTTP GET | `8080` | `/healthz` | Confirms the Python event loop and health server started. |
| Readiness | HTTP GET | `8080` | `/readyz` | Returns success only after the Discord bot is connected and ready. |
| Liveness | HTTP GET | `8080` | `/healthz` | Confirms the process remains responsive. |

A 15-second startup allowance and 30-second liveness interval are reasonable initial values. Increase the startup allowance if Discord connectivity from the selected region is slow.

## Health responses

- `GET /healthz` returns HTTP `200` with `{"status":"ok"}` while the process is responsive.
- `GET /readyz` returns HTTP `200` with `{"status":"ready"}` after Discord is connected, otherwise HTTP `503` with `{"status":"starting"}`.

The health service exposes no credentials or Bookshelf data.

## Discord configuration

The Discord application must have a bot user installed in the target server with permission to use application commands. Booky2.0 synchronizes its slash commands during startup.

The `/request` command accepts three options:

- `title` — search by audiobook title;
- `author` — search by author name;
- `collection` — choose **Yes** to request every book in the selected series.

Users can provide either title or author, or both. When both are supplied, Booky2.0 searches the combined phrase and each individual field, merges duplicate results, and ranks books that match both values first.

For a collection request, select any matching book in the intended series. Booky2.0 reads its series metadata, finds the other books, and starts an individual Bookshelf/Prowlarr request for each one without per-book confirmation. Progress and completion notices are posted in the request channel.

The `/absreport` command opens a private form with Audiobook Title, Audiobook Author, and Issue fields. The Issue field accepts up to 1,000 characters. Submitted reports are posted publicly in `ABS_REPORT_CHANNEL_ID`, while the submitter receives a private confirmation. Enable Discord Developer Mode to copy the destination channel ID, and give the bot View Channel, Send Messages, and Embed Links permissions there.
