## Contributing Guidelines

<p align="center">
  <img src="https://github.com/openebs/community/blob/develop/images/replicated-pv_mayastor_183x183_t.png">
</p>
<h3 align="center">
  Replicated PV Mayastor<BR>
</h3>

## Umbrella Project
OpenEBS is an umbrella project :open_umbrella:, where every project, repository and file in the OpenEBS organization adopts and follows the policies and principles of the umbrella project files located in our [``` Community repo cafe```](https://github.com/openebs/community?tab=readme-ov-file#community-repo-cafe).


| The <kbd>Replicated PV</kbd> ```Mayastor``` Storage Engine project adheres to the: [OpenEBS Contributor Guidelines](https://github.com/openebs/community/blob/HEAD/CONTRIBUTING.md) |
| :---: |
---
[![OpenEBS Welcome Banner](https://github.com/openebs/community/blob/develop/images/community_banner_retro_gamer_level-up-2024_transp.png)](https://www.openebs.io/)

| Quick links  |  [ ```CNCF openebs website ``` ](https://www.openebs.io/)  |  [ ```CNCF Product docs ``` ](https://www.openebs.io/docs)  | [  ``` Main Parent repo ```](https://github.com/openebs/openebs) |  [  ```Community Cafe```](https://github.com/openebs/community) | 
| :----        |              :---:             |            :---:             |            :---:             |            :---:             |


<BR>

> [!IMPORTANT]
> ## ```Contributing to MayaStor```
> We're excited that you are interested in contributing to the <kbd>Replicated PV</kbd> ```MayaStor``` Storage Engine. <BR>
> If you experience minor hurdles in contribution, **please report them.**

| [<img src="https://github.com/openebs/community/blob/develop/images/slack_icon_small.png" width="100">](https://kubernetes.slack.com/messages/openebs)  | **Try our Slack channel** <BR>If you have questions about using OpenEBS, please use the CNCF Kubernetes **OpenEBS slack channel**, it is open for [anyone to ask a question](https://kubernetes.slack.com/messages/openebs/) <BR> |
| :---         | :---      |

<BR>

Our interactions here are governed by the [openEBS Code of Conduct](https://github.com/openebs/community/blob/develop/CODE_OF_CONDUCT.md).

## Development Environment

Consult the [`doc/build.md`](doc/build.md) for a complete guide on who to get started contributing to the <kbd>Replicated PV</kbd> ```MayaStor``` Storage Engine.

---
<BR>

## Issues & Pull Requests

### ``` Reporting Bugs ```
Please try to report complete, well described, reproducible bugs to us. If you can't, that's ok. ```Do your best```.

- Our [Bug Report][issue-bug-report] template includes instructions on providing the information we need from you.

### ``` Requesting new features ```

You are invited to propose _complete, well described_ new features via...
- For a MAJOR NEW FEATURE, submit an OEP (openEBS Enhancement Proposal). Maintainers will review & discuss the new feature proposal and potentially schedule community discussions on it. The OEP process is new for us, so please bear with us on this.
- or submit a detailed ISSUE in the ``` community Cafe Repo ```
  - You must indicate if you are able to complete and support the features you propose.
- Note: Raising PR's as a <kbd>Feature Request</kbd> is not the process for requesting new features. Such PR's will be closed, and you will asked to resubmit as an OEP or an ISSUE.
   - All PR's are reviewed by the Maintainers and Eng/Dev first, before being approved and committed for Eng/development.


> The processes above are documented in the ```community Cafe repo``` discussion tab.
> If you can’t find them, please reach out one of the Maintainers below. We're happy to help.

<BR>

### Coding language & style
> [<img src="https://github.com/openebs/community/blob/develop/images/rust-lang-logo_rust.png" width="100">](https://www.rust-lang.org/learn) <BR>
> While the <kbd>Replicated PV</kbd> ```MayaStor``` Storage Engine has no formal RFC process, the [Rust RFC template][rust-rfc-template] is an excellent place to derive your issue description from.

| [<img src="https://github.com/openebs/community/blob/develop/images/rustacean-flat-happy.png" width="200">](https://www.rust-lang.org/)  | :crab: The <kbd>Replicated PV</kbd> ```MayaStor``` Storage Engine is ``` 100% ``` developed in the [RUST programming language](https://www.rust-lang.org/)<BR>:crab: We leverage guidance from the RUST community.<BR>:crab: We love Ferris.<BR>:crab: We are #rustaceans |
| :---         | :---      |

---
### Committing

You must start you Development work off the `develop` branch. Do **Not use `master`.** - (We do not use that branch name.)

| [<img src="https://github.com/openebs/community/blob/develop/images/rustacean-flat-gesture.png" width="200">](https://www.rust-lang.org/)  | :star: We run the [bors][bors] ```PR Merge bot tool```.<BR>:star: Bors will merge your commits once they has passed review.<BR>:star: Please ask if you need to [_squash merges_][squash-merges]. We generally do this. It's best to ask first.<BR>:star: Each commit message must adhere to [Conventional Commits][conventional-commits].<BR>:star: You can use [`convco`][tools-convco] if you need a tool to help with commit msgs.<BR>:star: It O.K to force push your branch if you need. Feel free to rewrite commit history it needed. |
| :---         | :---      |

---
### Reviews
| [<img src="https://github.com/openebs/community/blob/develop/images/rustacean-orig-noshadow.png" width="200">](https://www.rust-lang.org/)  | :zap: The review process is governed by [bors][bors].<BR>:zap: Pull requests require at least 2 approvals from Maintainers or SIG members.<BR>:zap: Once review is given, Maintainers and SIG members may indicate merge readiness.<BR>:zap: The comment message `bors merge` will auto trigger a merge.<BR>&nbsp; :warning: Do not hit the 'Update Branch' button.<BR>&nbsp; :warning: The commit message is not conventional, [bors][bors] will yell at you.<BR>&nbsp; :warning: Let [bors][bors] handle it, or rebase it yourself. |
| :---         | :---      |

---

| [<img src="https://github.com/openebs/community/blob/develop/images/slack_icon_small.png" width="100">](https://kubernetes.slack.com/messages/openebs)  | **Try our Slack channel** <BR>If you have questions about using OpenEBS, please use the CNCF Kubernetes **OpenEBS slack channel**, it is open for [anyone to ask a question](https://kubernetes.slack.com/messages/openebs/) <BR> |
| :---         | :---      |

---
## Project Leadership team
This Community is managed by the OpenEBS <kbd>Admins</kbd>, ```Maintainers``` and <kbd>**Senior leaders**</kbd> within the OpenEBS project team. We liaise with the Linux Foundation and CNCF on project, governance
and operational matters. We curate the daily operations of the project, product, roadmaps, initiatives, all engineering/code activities and all events (including conferences). Currently our leadership team is...

> | Name | GitHub | Geo | Role | Lead |
> | :--- | :--- | :--- | :--- | :--- |
> | [Vishnu Attur](https://www.linkedin.com/in/vishnu-attur-5309a333/ "Senior Engineering, QA and Dev Manager")| :octocat: <kbd>**[@avishnu](https://github.com/avishnu "Vishnu Govind Attur")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/in.png "I am based in Bengaluru, Karnataka, India (GMT+5:30) Timezone") | <kbd>**Admin**</kbd>, ```Maintainer``` | Eng/QA / PRs / Issues |
> | [Abhinandan Purkait](https://www.linkedin.com/in/abhinandan-purkait/ "Senior Engineer") | :sunglasses: <kbd>**[@Abhinandan-Purkait](https://github.com/Abhinandan-Purkait "Abhinandan Purkait")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/in.png "I am based in Bengaluru, Karnataka, India (GMT+5:30) Timezone") | ```Maintainer``` | PR's / Issues |
> | [Niladri Halder](https://www.linkedin.com/in/niladrih/ "Senior Engineer") | :rocket: <kbd>**[@niladrih](https://github.com/niladrih "Niladri Halder")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/in.png "I am based in Bengaluru, Karnataka, India (GMT+5:30) Timezone") | ```Maintainer``` | PR's / Issues |
> | [Ed Robinson](https://www.linkedin.com/in/edrob/ "CNCF Head Liaison") | :dog: <kbd>**[@edrob999](https://github.com/edrob999 "Ed Robinson")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/ni_tn/nz.png "I'm a Kiwi from New Zealand. I am based in San Francisco, USA (GMT-7) Timezone")  &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/to_zw/us.png "I am based in San Francisco, USA (GMT-7) Timezone") | <kbd>**CNCF Primary Liason**</kbd> | CNCF / Biz |
> | [Tiago Castro](https://www.linkedin.com/in/tiago-castro-3311453a/ "Chief Architect") | :zap: <kbd>**[@tiagolobocastro](https://github.com/tiagolobocastro "Tiago Castro")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/ni_tn/pt.png "I'm Portugueses. I'm based in London, UK (GMT+1) Timezone") &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/gb.png "I am based in London, UK (GMT+1) Timezone") | <kbd>**Admin**</kbd>, ```Maintainer``` |  PR's / Issues / Arch |
> | [David Brace](https://www.linkedin.com/in/dbrace/ "Head of Product Mgmt & Strategy") | :star: <kbd>**[@orville-wright](https://github.com/orville-wright "Dave Brace")**</kbd> | ![](https://github.com/openebs/community/blob/develop/images/flags/ni_tn/nz.png "I'm a Kiwi from New Zealand. I am based in San Francisco, USA (GMT-7) Timezone") &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/de_je/hu.png "I'm also Hungarian. I'm based in San Francisco, USA (GMT-7) Timezone") &nbsp; ![](https://github.com/openebs/community/blob/develop/images/flags/to_zw/us.png "Yes, I'm also an American. I'm based in San Francisco, USA (GMT-7) Timezone") | <kbd>**Admin**</kbd>, ```Maintainer``` | Prod / Issues / Biz |

<BR>

> [!Important]
> When creating an ISSUE or a PR. Please TAG one (or more) of the Maintainers.

Our Special Interest Groups (SIGs) are:
| Functional area | Name | @GitHub | Area of Expertise | Product specialization |
| :--- | :--- | :--- | :--- | :--- |
| ``` Entire Product ``` | Tiago Castro | @tiagolobocastro | Architect / Eng | Mayastor |
| ``` Data Plane ``` | Dmitry Savitskiy | @dsavitskiy | Architect / Eng | Mayastor |
| ``` e2e Testing ``` | Blaise Dias | @blaisedias | Eng / Test / QA | All products | 
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
- Yes, we have a set of guidelines and rules explaining how to advance to Maintainer-ship...
    - Governance rules, code of conduct, contributing and a ``` Maintainer Ladder ```  that details pre-reqs, criteria and responsibilities you need to agree to & meet
    - Contribution history is critical
    - PR review input, ISSUE management
    - Attendance at community meetings are also important
    - Feature contribution
    - Active support engagement on our SLACK Channel
    - Bug resolution

