# Contributing Guidelines
<BR>

## Umbrella Project
OpenEBS is an "umbrella project". Every project, repository and file in the OpenEBS organization adopts and follows the policies found in the Community repo umbrella project files.
<BR>

This project follows the [OpenEBS Contributor Guidelines](https://github.com/openebs/community/blob/HEAD/CONTRIBUTING.md)

# Contributing to MayaStor

We're excited to have you interested in contributing to MayaStor!

> If you experience minor hurdles in contribution, **please report them.**

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

| [<img src="https://github.com/openebs/community/blob/develop/images/slack_icon_small.png" width="100">](https://kubernetes.slack.com/messages/openebs)  | **Try our Slack channel** <BR>If you have questions about using OpenEBS, please use the CNCF Kubernetes **OpenEBS slack channel**, it is open for [anyone to ask a question](https://kubernetes.slack.com/messages/openebs/) <BR> |
| :---         | :---      |

---
## Project Leadership team
This Community is managed by the OpenEBS <kbd>Admins</kbd>, ```Maintainers``` and <kbd>**Senior leaders**</kbd> within the OpenEBS project team. We liaise with the Linux Foundation and CNCF on project, governance
and operational matters. We curate the daily operations of the project, product, roadmaps, initiatives, all engineering/code activities and all events (including conferences). Currently our leadership team is...

> | Name | Github | Geo | Role | Lead |
> | :--- | :--- | :--- | :--- | :--- |
> | [Vishnu Attur](https://www.linkedin.com/in/vishnu-attur-5309a333/ "Senior Engineering, QA and Dev Manager")| :octocat: <kbd>**[@avishnu](https://github.com/avishnu "Vishnu Govind Attur")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/in.png "I am based in Bengaluru, Karnataka, India (GMT+5:30) Timezone") | <kbd>**Admin**</kbd>, ```Maintainer``` | Eng/QA / PRs / Issues |
> | [Abhinandan Purkait](https://www.linkedin.com/in/abhinandan-purkait/ "Senior Engineer") | :sunglasses: <kbd>**[@Abhinandan-Purkait](https://github.com/Abhinandan-Purkait "Abhinandan Purkait")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/in.png "I am based in Bengaluru, Karnataka, India (GMT+5:30) Timezone") | ```Maintainer``` | PR's / Issues |
> | [Niladri Halder](https://www.linkedin.com/in/niladrih/ "Senior Engineer") | :rocket: <kbd>**[@niladrih](https://github.com/niladrih "Niladrih Halder")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/in.png "I am based in Bengaluru, Karnataka, India (GMT+5:30) Timezone") | ```Maintainer``` | PR's / Issues |
> | [Ed Robinson](https://www.linkedin.com/in/edrob/ "CNCF Head Liason") | :dog: <kbd>**[@edrob999](https://github.com/edrob999 "Ed Robinson")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/ni_tn/nz.png "I'm a Kiwi from New Zealand. I am based in San Francisco, USA (GMT-7) Timezone")  &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/to_zw/us.png "I am based in San Francisco, USA (GMT-7) Timezone") | <kbd>**CNCF Primary Liason**</kbd> | CNCF / Biz |
> | [Tiago Castro](https://www.linkedin.com/in/tiago-castro-3311453a/ "Chief Architect") | :zap: <kbd>**[@tiagolobocastro](https://github.com/tiagolobocastro "Tiago Castro")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/ni_tn/pt.png "I'm Portugueses. I'm based in London, UK (GMT+1) Timezone") &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/gb.png "I am based in London, UK (GMT+1) Timezone") | <kbd>**Admin**</kbd>, ```Maintainer``` |  PR's / Issues / Arch |
> | [David Brace](https://www.linkedin.com/in/dbrace/ "Head of Product Mgmt & Strategy") | :star: <kbd>**[@orville-wright](https://github.com/orville-wright "Dave Brace")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/ni_tn/nz.png "I'm a Kiwi from New Zealand. I am based in San Francisco, USA (GMT-7) Timezone") &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/hu.png "I'm also Hungarian. I'm based in San Francisco, USA (GMT-7) Timezone") &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/to_zw/us.png "Yes, I'm also an American. I'm based in San Francisco, USA (GMT-7) Timezone") | <kbd>**Admin**</kbd>, ```Maintainer``` | Prod / Issues / Biz |

<BR>

> [!Important]
> When creating an ISSUE or a PR. Please TAG one (or more) of the Maintainers.

Our Special Interest Groups (SIGs) are:
| Functional area | Name | @GitHub | Are of Expertese | Product specialization |
| :--- | :--- | :--- | :--- | :--- |
| ``` Entire Product ``` | Tiago Castro | @tiagolobocastro | Architect / Eng | Mayastor |
| ``` Data Plane ``` | Dimitry Savitskiy | @dsavitskiy | Architect / Eng | Mayastor |
| ``` Data Plane ``` | Jonathan Teh | @jonathan-teh | Eng | Mayastor |
| ``` Data Plane ``` | Mikhail Tcymbaliuk | @mtzaurus | Archietct / Eng | Mayastor |
| ``` Control Plane ``` | Paul Yoong | @paulyoong | Archietct / Eng | Mayastor |
| ``` 2e2 Testing ``` | Blaise Dias | @blaisedias | Eng / Test / QA | All products | 
| ``` e2e Testing ``` | Chris Denyer | @chriswldenyer | Eng / QA | Mayastor |
 
---

> [!Important]
> FAQ's on SIG's, Contributors and Maintainers 
<BR>

## What is a _Special Interest Group (SIG)_?
- SIGs are small teams working together on a specific area/topic/zone.
- They may change at any time, and have no strict definition.
- SIGs may be created, empowered, and destroyed by the maintainers at any time.


## May I join a SIG?
- Of course, we'd love that!


## May I Become a maintainer?
- Yes, we have a set of guidelines and rules explaing how to advnace to Maintainership...
    - Governance rules, code of coduct, contributing and a ``` Maintainer Ladder ```  that details pre-reqs, criteria and responsibilitis you need to agree to & meet
    - Contribution history is critical
    - PR review input, ISSUE managment
    - Attendance at community meetings are also important
    - Feature contribnution
    - Active support enaggement on our SLACK Channel
    - Bug resolution


