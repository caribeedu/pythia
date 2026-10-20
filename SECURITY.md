# Security policy

Pythia handles credentials (an Anthropic API key and tokens for GitHub, Slack, Confluence and incident.io) and the content of private conversations, so security reports are taken seriously.

## Reporting a vulnerability

Please do **not** open a public issue. Use GitHub's private vulnerability reporting: open the **Security** tab of the repository and choose **Report a vulnerability**.

Include what you found, how to reproduce it and the impact you expect. You will get an acknowledgement, and fixes are prioritized by severity.

## Design notes relevant to security

- Pythia runs locally. The web interface binds to `127.0.0.1` and has no authentication, so do not expose it to a network.
- Secrets are stored in the operating system keychain, never in the database or in files.
- Source content is kept outside the append-only event log so that a deletion at the source can erase it, together with every derived text.
- Connectors only perform read operations on the sources and are meant to be used with read-only, user-level tokens.
