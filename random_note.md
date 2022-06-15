### set up VM

1. create ssh key e.g., gcp
2. connect to your VM - two option
    2.1 via ssh command
    ```bash
    ssh -i  ~/.ssh/gcp <name>@<IP>
    ```

    2.2 create `config` in .ssh directory
    ```bash
    HOST <alias name>
        HostName <IP>
        User <name>
        IndentityFile <.ssh directory>
    ```

3. after setup `config` you should be able to access VM with

```bash
ssh <alias name>
```

### Connect with your VM on vscode
1. install extension `remote ssh`
2. open remote window (left side corner)
3. Connect to host. (Since we have config file the host should shown on panel)

### to close VM instance
i) you can do `sudo shutdown now`
ii) you can also manually stop in gcloud ui

### Resume
1. copy External IP
2. Go to your terminal `nano .ssh/config`
3. replace `Hostname` with new one
*note: something you can just run `ssh` command without change hostname*

### Remove
1. Go to gcloud ui and click remove
ya, that's it

### other stuff

- Install anaconda
    1. donnload .sh from anaconda official
    2. run `bash anaconda...sh`

- Install sudo
    1. run `sudo apt-get update`

- Install docker
    1. run `sudo apt-get install docker.io`

- No sudo docker

[git](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md)

```bash
$ sudo groupadd docker
$ sudo gpasswd -a $USER docker
$ sudo service docker restart
$ sudo service docker.io restart
```

- Install docker-compose
    1. Go to [git](https://github.com/docker/compose/releases)
    2. Download the version match your system (`docker-compose-linux-x86_64` in my case)
    3. create `bin/` with `mkdir bin` and cd to it `cd bin`
    4. Download file `wget https://github.com/docker/compose/releases/download/v2.6.0/docker-compose-linux-x86_64 -O docker-compose`
    5. made file executable with `chmod +x docker-compose`
    6. set path env
        6.1 `nano .bashrc` 
        6.2 go to lowest line and type `export PATH="${HOME}/bin:${PATH}"`
        6.3 press Crtl+O to save
        6.4 press Crtl+X to exit

- Install terraform linux
    1. go to [terraform](https://www.terraform.io/downloads)
    2. copy [AMD64](https://releases.hashicorp.com/terraform/1.2.2/terraform_1.2.2_linux_amd64.zip)
    4. `cd bin`
    3. `wget https://releases.hashicorp.com/terraform/1.2.2/terraform_1.2.2_linux_amd64.zip`
    4. `unzip terraform..` *note: you may need to install unzip with `sudo apt-get install unzip`*
    5. `rm terrafor_1..` remove zip file from your directory
    6. used `sftp <alias name>` to connect to your sftp VM
    7. `put credential.json`
    8. `export GOOGLE_APPLICATION_CREDENTIALS=~/<directory>/google_credentials.json`
    9. `gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS`
    10. try `terraform init`