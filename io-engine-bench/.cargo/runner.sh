#! /usr/bin/env bash

# Grab the arguments passed to the runner.
ARGS="${@}"

if [[ $EUID -ne 0 ]]; then
  MAYBE_SUDO='sudo -E'
else
  MAYBE_SUDO=''
fi

TARGET_CRITERION="$SRCDIR/target/criterion"
GIT_CRITERION="$SRCDIR/io-engine-bench/results/criterion"

if [ -d "$GIT_CRITERION" ]; then
    mv "$GIT_CRITERION" "$TARGET_CRITERION"
fi

# Elevate to sudo so we can set some capabilities via `capsh`, then execute the args with the required capabilities:
#
# * Set `cap_setpcap` to be able to set [ambient capabilities](https://lwn.net/Articles/636533/) which can be inherited
# by children.
# * Set `cap_sys_admin,cap_ipc_lock,cap_sys_nice` as they are required by the io-engine.
${MAYBE_SUDO} capsh \
  --caps="cap_setpcap+iep cap_sys_admin,cap_ipc_lock,cap_sys_nice+iep" \
  --addamb=cap_sys_admin --addamb=cap_ipc_lock --addamb=cap_sys_nice \
  -- -c "${ARGS}"

if [ -d "$TARGET_CRITERION" ]; then
    ${MAYBE_SUDO} chown -R $USER "$TARGET_CRITERION" &>/dev/null | true
    mv "$TARGET_CRITERION" "$GIT_CRITERION"
fi
