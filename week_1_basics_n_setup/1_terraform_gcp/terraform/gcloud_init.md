# Google Cloud

## Initialization

```bash
(base) ➜  data-engineering-zoomcamp git:(clamytoe) gcloud init
Welcome! This command will take you through the configuration of gcloud.

Your current configuration has been set to: [default]

You can skip diagnostics next time by using the following flag:
  gcloud init --skip-diagnostics

Network diagnostic detects and fixes local network connection issues.
Checking network connection...done.
Reachability Check passed.
Network diagnostic passed (1/1 checks passed).

You must log in to continue. Would you like to log in (Y/n)?

Your browser has been opened to visit:

    https://accounts.google.com/o/oauth2/auth?response_type=code&client_id=32555940559.apps.googleusercontent.com&redirect_uri=http%3A%2F%2Flocalhost%3A8085%2F&scope=openid+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fuserinfo.email+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fappengine.admin+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fsqlservice.login+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcompute+https%3A%2F%2Fwww.googleapis.com%2Fauth%2Faccounts.reauth&state=Eaw6q2VmkjnOSOlK07ERs8Swb2cQ2I&access_type=offline&code_challenge=ZySH1I8YCDhkz92x_5c0hssnRnGHTjFZPMeUYiijNwk&code_challenge_method=S256

tcgetpgrp failed: Not a tty
You are logged in as: [clamytoe@gmail.com].

Pick cloud project to use:
 [1] dtc-de-course-374214
 [2] studied-limiter-374215
 [3] Enter a project ID
 [4] Create a new project
Please enter numeric choice or text value (must exactly match list item):  1

Your current project has been set to: [dtc-de-course-374214].

Not setting default zone/region (this feature makes it easier to use
[gcloud compute] by setting an appropriate default value for the
--zone and --region flag).
See https://cloud.google.com/compute/docs/gcloud-compute section on how to set
default compute region and zone manually. If you would like [gcloud init] to be
able to do this for you the next time you run it, make sure the
Compute Engine API is enabled for your project on the
https://console.developers.google.com/apis page.

Created a default .boto configuration file at [/home/clamytoe/.boto]. See this file and
[https://cloud.google.com/storage/docs/gsutil/commands/config] for more
information about configuring Google Cloud Storage.
Your Google Cloud SDK is configured and ready to use!

* Commands that require authentication will use clamytoe@gmail.com by default
* Commands will reference project `dtc-de-course-374214` by default
Run `gcloud help config` to learn how to change individual settings

This gcloud configuration is called [default]. You can create additional configurations if you work with multiple accounts and/or projects.
Run `gcloud topic configurations` to learn more.

Some things to try next:

* Run `gcloud --help` to see the Cloud Platform services you can interact with. And run `gcloud help COMMAND` to get help on any gcloud command.
* Run `gcloud topic --help` to learn about advanced features of the SDK like arg files and output formatting
* Run `gcloud cheat-sheet` to see a roster of go-to `gcloud` commands.
```

## Auth List

```bash
(base) ➜  data-engineering-zoomcamp git:(clamytoe) gcloud auth list
  Credentialed Accounts
ACTIVE  ACCOUNT
*       clamytoe@gmail.com

To set the active account, run:
    $ gcloud config set account `ACCOUNT`
```

## Config List

```bash
(base) ➜  data-engineering-zoomcamp git:(clamytoe) ✗ gcloud config list
[core]
account = clamytoe@gmail.com
disable_usage_reporting = True
project = dtc-de-course-374214

Your active configuration is: [default]
```

## Info

```bash
(base) ➜  data-engineering-zoomcamp git:(clamytoe) ✗ gcloud info
Google Cloud SDK [412.0.0]

Platform: [Linux, x86_64] uname_result(system='Linux', node='ROG-STRIX', release='5.15.79.1-microsoft-standard-WSL2', version='#1 SMP Wed Nov 23 01:01:46 UTC 2022', machine='x86_64')
Locale: ('en_US', 'UTF-8')
Python Version: [3.9.12 (main, Apr 30 2022, 03:04:12)  [Clang 12.0.1 ]]
Python Location: [/usr/bin/../lib/google-cloud-sdk/platform/bundledpythonunix/bin/python3]
OpenSSL: [OpenSSL 1.1.1l  24 Aug 2021]
Requests Version: [2.25.1]
urllib3 Version: [1.26.9]
Default CA certs file: [/usr/bin/../lib/google-cloud-sdk/lib/third_party/certifi/cacert.pem]
Site Packages: [Enabled]

Installation Root: [/usr/lib/google-cloud-sdk]
Installed Components:
  alpha: [2022.12.09]
  core: [2022.12.09]
  bq: [2.0.83]
  gsutil: [5.17]
  gcloud-crc32c: [1.0.0]
  beta: [2022.12.09]
  bundled-python3-unix: [3.9.12]
System PATH: [/home/clamytoe/bin:/home/clamytoe/miniconda3/bin:/home/clamytoe/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/lib/wsl/lib:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v11.8/bin:/mnt/c/Program Files/NVIDIA GPU Computing Toolkit/CUDA/v11.8/libnvvp:/mnt/c/Program Files/Oculus/Support/oculus-runtime:/mnt/c/Windows/system32:/mnt/c/Windows:/mnt/c/Windows/System32/Wbem:/mnt/c/Windows/System32/WindowsPowerShell/v1.0:/mnt/c/Windows/System32/OpenSSH:/mnt/c/Program Files/Intel/WiFi/bin:/mnt/c/Program Files/Common Files/Intel/WirelessCommon:/mnt/c/WINDOWS/system32:/mnt/c/WINDOWS:/mnt/c/WINDOWS/System32/Wbem:/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0:/mnt/c/WINDOWS/System32/OpenSSH:/mnt/c/Program Files (x86)/NVIDIA Corporation/PhysX/Common:/mnt/c/Program Files/NVIDIA Corporation/NVIDIA NvDLISR:/mnt/c/Program Files/Git/cmd:/mnt/c/ffmpeg/bin:/mnt/c/Program Files/dotnet:/mnt/c/WINDOWS/system32:/mnt/c/WINDOWS:/mnt/c/WINDOWS/System32/Wbem:/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0:/mnt/c/WINDOWS/System32/OpenSSH:/mnt/c/Program Files/Amazon/AWSCLIV2:/mnt/c/Program Files/NVIDIA Corporation/Nsight Compute 2022.3.0:/mnt/c/Program Files/PowerShell/7:/mnt/c/Program Files/Docker/Docker/resources/bin:/mnt/c/Users/clamy/anaconda3:/mnt/c/Users/clamy/anaconda3/Library/mingw-w64/bin:/mnt/c/Users/clamy/anaconda3/Library/usr/bin:/mnt/c/Users/clamy/anaconda3/Library/bin:/mnt/c/Users/clamy/anaconda3/Scripts:/mnt/c/Users/clamy/AppData/Local/Microsoft/WindowsApps:/mnt/c/Users/clamy/AppData/Local/Programs/Microsoft VS Code/bin:/mnt/c/Program Files/JetBrains/PyCharm 2021.3.3/bin:/mnt/c/Users/clamy/AppData/Local/Programs/MiKTeX/miktex/bin/x64:/mnt/c/jdk-18.0.1.1/bin:/mnt/c/Users/clamy/AppData/Local/Programs/Quarto/bin:/mnt/c/Users/clamy/AppData/Local/Pandoc]
Python PATH: [/usr/bin/../lib/google-cloud-sdk/lib/third_party:/usr/lib/google-cloud-sdk/lib:/usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python39.zip:/usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9:/usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9/lib-dynload:/usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9/site-packages:/usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9/site-packages/setuptools-57.4.0-py3.9.egg:/usr/lib/google-cloud-sdk/platform/bundledpythonunix/lib/python3.9/site-packages/pip-21.1.3-py3.9.egg:/home/clamytoe/.local/lib/python3.9/site-packages]
Cloud SDK on PATH: [False]
Kubectl on PATH: [/usr/local/bin/kubectl]

Installation Properties: [/usr/lib/google-cloud-sdk/properties]
User Config Directory: [/home/clamytoe/.config/gcloud]
Active Configuration Name: [default]
Active Configuration Path: [/home/clamytoe/.config/gcloud/configurations/config_default]

Account: [clamytoe@gmail.com]
Project: [dtc-de-course-374214]

Current Properties:
  [core]
    account: [clamytoe@gmail.com] (property file)
    disable_usage_reporting: [True] (property file)
    project: [dtc-de-course-374214] (property file)

Logs Directory: [/home/clamytoe/.config/gcloud/logs]
Last Log File: [/home/clamytoe/.config/gcloud/logs/2023.01.09/10.56.55.405343.log]

git: [git version 2.38.1]
ssh: [OpenSSH_8.2p1 Ubuntu-4ubuntu0.5, OpenSSL 1.1.1f  31 Mar 2020]
```
