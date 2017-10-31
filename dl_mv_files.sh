#!/bin/bash
#Dean - Created: 20170215 Last updated: 20170216

msg()
{
  echo $1 >> /data01/infa/PROD/CDR/dl_mv_files.log
}
        msg "---Script started on `date`---"

###PARAMETERS###
backlog_dir=$1 #Full directory path where you want to move files from. Path must start with /
tgt_dir=$2 #Full directory path where you want to move files to. Path must start with /
nr_files_tgt_limit=$3 #The limit of files which is allowed to be in the target dir. It will not move more files in there than this limit. If you want to move all of the files from the source and not care about how many files end up in the target dir, pass a high number such as 999999999
file_wildcard=$4 #File Wildcard for the ls cmd in the source/backlog dir and to do counts in the target and source dir.


nr_scripts_busy=`ps -ef | grep dl_mv | grep $backlog_dir | wc -l`

if [ "$nr_scripts_busy" -gt 2 ] ; then #for some reason when the script kicks off itself, it sees two sessions. So in this case, 2=1. 
 nr_scripts_busy=$(($nr_scripts_busy - 2))
  msg "WARNING - Already $nr_scripts_busy instance of this script running to move files from $backlog_dir. Exiting!" 
  exit 1 
else
 nr_scripts_busy=$(($nr_scripts_busy - 2))
  msg "Nr scripts busy moving files from $backlog_dir is $nr_scripts_busy. Continuing..." 
fi

cd $tgt_dir
 tgt_file_cnt=`ls | grep $file_wildcard | wc -l`

nr_files_to_mv="$(($nr_files_tgt_limit - $tgt_file_cnt))"

if [ $tgt_file_cnt -lt $nr_files_tgt_limit ] ; then cd $backlog_dir
 if [ "$?" -eq 0 ]; then
backlog_dir_cnt=`ls | grep $file_wildcard | wc -l`
   if [ "$backlog_dir_cnt" -lt "$nr_files_to_mv" ] ; then
      msg "Move of $backlog_dir_cnt files STARTED to $tgt_dir from $backlog_dir with file wildcard $file_wildcard"
      ls | grep $file_wildcard | head -n $backlog_dir_cnt | xargs -i mv {} $tgt_dir;
      msg "Move of $backlog_dir_cnt files COMPLETED to $tgt_dir from $backlog_dir with file wildcard $file_wildcard"
   else
      msg "Move of $nr_files_to_mv files STARTED to $tgt_dir from $backlog_dir with file wildcard $file_wildcard"
       ls | grep $file_wildcard | head -n $nr_files_to_mv | xargs -i mv {} $tgt_dir;
      msg "Move of $nr_files_to_mv files COMPLETED to $tgt_dir from $backlog_dir with file wildcard $file_wildcard"
   fi
 fi
else
    msg "WARNING - File count in $tgt_dir of $tgt_file_cnt greater than target dir limit of $nr_files_tgt_limit. Exiting!" 
fi
        msg "---Script completed on `date`---"
