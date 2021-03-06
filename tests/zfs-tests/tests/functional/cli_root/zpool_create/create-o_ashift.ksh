#!/bin/ksh -p
#
# CDDL HEADER START
#
# The contents of this file are subject to the terms of the
# Common Development and Distribution License (the "License").
# You may not use this file except in compliance with the License.
#
# You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
# or http://www.opensolaris.org/os/licensing.
# See the License for the specific language governing permissions
# and limitations under the License.
#
# When distributing Covered Code, include this CDDL HEADER in each
# file and include the License file at usr/src/OPENSOLARIS.LICENSE.
# If applicable, add the following below this CDDL HEADER, with the
# fields enclosed by brackets "[]" replaced with your own identifying
# information: Portions Copyright [yyyy] [name of copyright owner]
#
# CDDL HEADER END
#

#
# Copyright 2016, loli10K. All rights reserved.
# Copyright (c) 2017 Datto Inc.
#

. $STF_SUITE/include/libtest.shlib
. $STF_SUITE/tests/functional/cli_root/zpool_create/zpool_create.shlib

#
# DESCRIPTION:
#	'zpool create -o ashift=<n> ...' should work with different ashift
#	values.
#
# STRATEGY:
#	1. Create various pools with different ashift values.
#	2. Verify -o ashift=<n> works only with allowed values (9-16).
#	   Also verify that the lowest number of uberblocks in a label is 16 and
#	   smallest uberblock size is 8K even with higher ashift values.
#

verify_runnable "global"

function cleanup
{
	poolexists $TESTPOOL && destroy_pool $TESTPOOL
	log_must rm -f $disk
}

#
# Commit the specified number of TXGs to the provided pool
# We use 'zpool sync' here because we can't force it via sync(1) like on illumos
# $1 pool name
# $2 number of txg syncs
#
function txg_sync
{
	typeset pool=$1
	typeset -i count=$2
	typeset -i i=0;

	while [ $i -lt $count ]
	do
		log_must sync_pool $pool true
		((i = i + 1))
	done
}

#
# Verify device $1 labels contains $2 valid uberblocks in every label
# $1 device
# $2 uberblocks count
#
function verify_device_uberblocks
{
	typeset device=$1
	typeset ubcount=$2

	zdb -quuul $device | egrep '^(\s+)?Uberblock' |
	    egrep -v 'invalid$' | awk \
	    -v ubcount=$ubcount '{ uberblocks[$0]++; }
	    END { for (i in uberblocks) {
		count++;
		if (uberblocks[i] != 4) { exit 1; }
	    }
	    if (count != ubcount) { exit 1; } }'

	return $?
}

log_assert "zpool create -o ashift=<n>' works with different ashift values"
log_onexit cleanup

disk=$TEST_BASE_DIR/$FILEDISK0
log_must mkfile $SIZE $disk

typeset ashifts=("9" "10" "11" "12" "13" "14" "15" "16")
# since Illumos 4958 the largest uberblock is 8K so we have at least of 16/label
typeset ubcount=("128" "128" "64" "32" "16" "16" "16" "16")
typeset -i i=0;
while [ $i -lt "${#ashifts[@]}" ]
do
	typeset ashift=${ashifts[$i]}
	log_must zpool create -o ashift=$ashift $TESTPOOL $disk
	typeset pprop=$(get_pool_prop ashift $TESTPOOL)
	verify_ashift $disk $ashift
	if [[ $? -ne 0 || "$pprop" != "$ashift" ]]
	then
		log_fail "Pool was created without setting ashift value to "\
		    "$ashift (current = $pprop)"
	fi
	# force 128 txg sync to fill the uberblock ring
	txg_sync $TESTPOOL 128
	verify_device_uberblocks $disk ${ubcount[$i]}
	if [[ $? -ne 0 ]]
	then
		log_fail "Pool was created with unexpected number of uberblocks"
	fi
	# clean things for the next run
	log_must zpool destroy $TESTPOOL
	log_must zpool labelclear $disk
	log_must eval "verify_device_uberblocks $disk 0"
	((i = i + 1))
done

typeset badvals=("off" "on" "1" "8" "17" "1b" "ff" "-")
for badval in ${badvals[@]}
do
	log_mustnot zpool create -o ashift="$badval" $TESTPOOL $disk
done

log_pass "zpool create -o ashift=<n>' works with different ashift values"
