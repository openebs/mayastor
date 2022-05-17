# Microk8s

[Microk8s](https://microk8s.io) is a very useful tool for K8s development, it is lightweight, very easy to install, has
support for multiple versions and is easy to reset.

## QuickStart microk8s

1. Install k8s using microk8s:

   ```bash
   snap install microk8s --classic --channel=1.14/stable
   ```

1. Update the configuration of microk8s to allow for privileged mode:
   **_note_ for newer versions the allow-privileged flag is removed**

   ```bash
   # kubelet config
   # - add `--allow-privileged=true`
   systemctl restart snap.microk8s.daemon-kubelet.service

   #kube-apiserver config
   # - add `--allow-privileged=true`
   systemctl restart snap.microk8s.daemon-apiserver.service
   ```

1. Verify the changes were successfully applied:

   ```bash
   ~snap services
   Service                             Startup  Current  Notes
   microk8s.daemon-apiserver           enabled  active   -
   microk8s.daemon-apiserver-kicker    enabled  active   -
   microk8s.daemon-containerd          enabled  active   -
   microk8s.daemon-controller-manager  enabled  active   -
   microk8s.daemon-etcd                enabled  active   -
   microk8s.daemon-kubelet             enabled  active   -
   microk8s.daemon-proxy               enabled  active   -
   microk8s.daemon-scheduler           enabled  active   -
   ```

1. If required make sure forwarding rules are enabled:

   ```bash
   sudo iptables -P FORWARD ACCEPT
   ```

1. Make _sure_ you update the arguments in the YAML file to point to the proper microk8s directories.
   For example:

   ```yaml
   - name: registration-dir
     hostPath:
       path: /var/snap/microk8s/common/var/lib/kubelet/plugins_registry/
       type: Directory
   ```

Notice how the path now is prepended with `/var/snap/microk8s/common/` compared to the original file.
Depending on what distribution you are using, this might slightly differ.
