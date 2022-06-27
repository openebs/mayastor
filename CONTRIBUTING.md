# Contributing to MayaStor

We're excited to have you interested in contributing to MayaStor!

> _Disclaimer:_ MayaStor is a **beta** project, and contributors at this stage of the project
> lifecycle may experience minor hurdles to contribution.
>
> **We want to overcome these. Please report them.**

If you have any questions, our ecosystem can be connected with over [Discord][mayastor-discord]
(for development) and [Slack][mayastor-slack] ([invite][mayastor-slack-inviter], for support).

Our interactions here are governed by the [CNCF Code of Conduct](CODE-OF_CONDUCT.md).

## Development Environment

Consult the [`doc/build.md`](doc/build.md) for a complete guide to getting started contributing
to MayaStor.

## Issues & Pull Requests

### Reporting Bugs

You would be **the best** if you reported complete, well described, reproducible bugs to us. If
you can't, that's ok. Do your best.

Our [Bug Report][issue-bug-report] template includes instructions to get the the information we
need from you.

### Requesting new features

You are invited to open _complete, well described_ issues proposing new features. While MayaStor
has no formal RFC process at this time, the [Rust RFC template][rust-rfc-template] is an
excellent place to derive your issue description from.

**You should indicate if you are able to complete and support features you propose.**

### Committing

Start work off the `develop` branch. **Not `master`.**

[bors][bors] will merge your commits. We do not do [_squash merges_][squash-merges].

Each commit message must adhere to [Conventional Commits][conventional-commits]. You can use
[`convco`][tools-convco] if you would prefer a tool to help.

It is absolutely fine to force push your branch if you need. Feel free to rewrite commit history
of your pull requests.

### Reviews

The review process is governed by [bors][bors].

Pull requests require at least 1 approval from maintainer or SIG member.

Once review is given, Maintainers and SIG members may indicate merge readiness with the comment
`bors merge`.

**Please do not hit the 'Update Branch' button.** The commit message is not conventional,
[bors][bors] will yell at you. Let [bors][bors] handle it, or rebase it yourself.

## Organization

Our maintainers are:

- [@gila][members-gila] - [@mayadata-io][maya-data]
- [@jkryl][members-jkryl] - [@mayadata-io][maya-data]
- [@GlennBullingham][members-glennbullingham] - [@mayadata-io][maya-data]

Our Special Interest Groups (SIGs) are:

- Dataplane
  - [@hoverbear][members-hoverbear] - [@mayadata-io][maya-data] &
    [@Hoverbear-Consulting](https://github.com/Hoverbear-Consulting)
  - [@mtzaurus][members-mtzaurus] - [@mayadata-io][maya-data]
  - [@jonathan-teh][members-jonathan-teh] - [@mayadata-io][maya-data]
- e2e-testing
  - [@chriswldenyer][members-chriswldenyer] - [@mayadata-io][maya-data]
  - [@blaisedias][members-blaisedias] - [@mayadata-io][maya-data]
- Control Plane
  - [@tiagolobocastro][members-tiagolobocastro] - [@mayadata-io][maya-data]
  - [@paulyoong][members-paulyoong] - [@mayadata-io][maya-data]

Former SIGs (and their members) are:

- None, yet!

### Organization FAQs

- **What is a _Maintainer_?**

  Maintainers are the project architects. They have the final say on what features get accepted,
  what code gets merged, when releases are cut, and how the project evolves.

  Maintainers **must** make decisions unanimously, no majorities, no votes.

- **What is a _Special Interest Group (SIG)_?**

  SIGs are small ephemeral teams (max 7) working on a general topic.

  They may change at any time, and have no strict definition.

  SIGs may be created, empowered, and destroyed by the maintainers at any time.

- **Are there other levels/roles/organization structure?**

  No. We want to focus on building MayaStor.

  It's preferable that we flow like water as opposed to become a rue goldberg machine of rules.

- **May I join a SIG? Become a maintainer?**

  Of course, we'd love that!

  Once you have a bit of contribution history with the project you will probably already find
  yourself working with a SIG, so ask, and they'll include you.

  Once you have acted as part of multiple SIGs, contributed at least one major feature, and
  resolved multiple bug reports, our maintainers may choose to include you in their midst.

[maya-data]: https://github.com/mayadata-io/
[mayastor-discord]: https://discord.gg/nhpyMeJCHE
[mayastor-slack]: https://kubernetes.slack.com/messages/openebs
[mayastor-slack-inviter]: https://slack.k8s.io/
[members-gila]: https://github.com/gila
[members-jkryl]: https://github.com/jkryl
[members-glennbullingham]: https://github.com/GlennBullingham
[members-hoverbear]: https://github.com/hoverbear
[members-tiagolobocastro]: https://github.com/tiagolobocastro
[members-mtzaurus]: https://github.com/mtzaurus
[members-jonathan-teh]: https://github.com/jonathan-teh
[members-paulyoong]: https://github.com/paulyoong
[members-chriswldenyer]: https://github.com/chriswldenyer
[members-blaisedias]: https://github.com/blaisedias
[rust-rfc-template]: https://github.com/rust-lang/rfcs/blob/master/0000-template.md
[issue-bug-report]: https://github.com/openebs/Mayastor/issues/new?labels=new&template=bug_report.md
[bors]: https://bors.tech/
[squash-merges]: https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-request-merges#squash-and-merge-your-pull-request-commits
[conventional-commits]: https://www.conventionalcommits.org/en/v1.0.0/
[tools-convco]: https://convco.github.io/
