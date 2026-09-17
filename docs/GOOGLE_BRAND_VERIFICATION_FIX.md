# Google brand-verification remediation

The reviewer rejected the original submission because the root domain was parked,
the submitted pages were rendered inside Streamlit, and the Google app name did not
match the visible application name. The repository now includes a plain-HTML public
site in `public_site/` and a GitHub Pages deployment workflow.

## 1. Publish the public site

1. Push the `heroku` branch to GitHub.
2. Open the repository **Settings → Pages**.
3. Under **Build and deployment**, select **GitHub Actions**.
4. Run the **Deploy public verification site** workflow if it did not start
   automatically.
5. In **Settings → Pages → Custom domain**, enter `admontracker.online`.

## 2. Replace Porkbun parking DNS

In Porkbun DNS, remove the parking records for the root (`@`). Do not change the
existing `app` CNAME used by Heroku.

Add these four GitHub Pages `A` records:

| Type | Host | Answer |
| --- | --- | --- |
| A | @ | 185.199.108.153 |
| A | @ | 185.199.109.153 |
| A | @ | 185.199.110.153 |
| A | @ | 185.199.111.153 |

For `www`, add `CNAME` host `www` with answer `edenede2.github.io`.

After DNS and certificate provisioning complete, enable **Enforce HTTPS** in the
GitHub Pages settings. Confirm that the root domain serves AdmonTracker instead of
the Porkbun “Coming Soon” page.

## 3. Verify domain ownership

Using a Google account that is an Owner or Editor of Cloud project
`admontracker`:

1. Open Google Search Console.
2. Add a **Domain property**, not a URL-prefix property.
3. Enter `admontracker.online`.
4. Copy Search Console's `google-site-verification=...` TXT value.
5. In Porkbun DNS, add a `TXT` record with host `@` and that exact value.
6. Return to Search Console and select **Verify**.
7. Keep the TXT record in DNS after verification.

## 4. Correct Google Auth Platform branding

In project `admontracker`, open **Google Auth Platform → Branding** and set:

- App name: `AdmonTracker`
- Homepage: `https://admontracker.online/`
- Privacy policy: `https://admontracker.online/privacy.html`
- Terms of service: `https://admontracker.online/terms.html`
- Authorized domain: `admontracker.online`
- Developer and support contact: a monitored University account

Do not change the working application redirects:

- Staff: `https://app.admontracker.online/oauth2callback`
- Participant: `https://app.admontracker.online/?google_health_callback=1`

Open all three public URLs in a signed-out/private browser window before requesting
re-verification. They must return the final HTML site without redirecting to the
application or requesting login.
