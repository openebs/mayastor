{ config, stdenv, pkgs, lib, ... }:

with lib;

{
  options = {
    services.iscsid = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "enable iscsid running on startup";
      };
    };
  };
  config = mkIf config.services.iscsid.enable {
    systemd.services.iscsid = {
      description = "iscsid daemon";
      wantedBy = [ "basic.target" ];
      serviceConfig = {
        ExecStart =
          "${pkgs.openiscsi}/bin/iscsid -f -c ${pkgs.openiscsi}/etc/iscsi/iscsid.conf -i ${pkgs.openiscsi}/etc/iscsi/initiatorname.iscsi";
        KillMode = "process";
        Restart = "on-success";
      };
    };
  };
}
