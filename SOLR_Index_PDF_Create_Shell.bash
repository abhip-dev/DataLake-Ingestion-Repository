#############################################################################################################################################################
#  Script Shell : bash
#  Script       : rrd_pdf_customer_letters_ksh
#
#  Description  : FTP Pattern modified ingestion for pdf files for customers welcome letters
#                 The script watches a particular directory for a specific files and ingests them every day at 9:00 pm
#
#  Author       : Gunasekharan Narayana
#
#############################################################################################################################################################

#!/bin/bash
#set -x


#########################################  usage display #########################################

usage()
{
    echo "USAGE: rrd_pdf_customer_letters_ksh FTP_SOURCE_NAME FTP_SOURCE_DIR HDFS_TARGET_DIR SCREEN_AND_LOG "
    echo "FTP_SOURCE_NAME like drp, mkltr, media_room etc like what is the name of source"
    echo "FTP_SOURCE_DIR  this is like, where to pick the source ftp files from. which dir they are pushed from source system"
    echo "HDFS_TARGET_DIR which dir in HDFS cluster we want to push the files at"
    echo "progress status info type LOG, EMAIL or SCREEN_AND_LOG"

}


######################################### checks made for if directory exists in HDFS or UNIX #########################################
check_dir_exists()
{
  echo "Start check_dir_exists function " >>  ${log}
  given_dir=${1}
  given_test=${2}
  given_type=${3}
  lcl_email_log_option=${4}


  #### check for UNIX type #####
  if [ ${given_type} = 'UNIX_DIR' ]; then

    if [ ${given_test} = 'VALIDATE'  ]; then

       if test ! -d ${given_dir}
         then
           MSG=" Failed : provided Dir ${given_dir}  does not exists. Terminating"
           msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
           exit 1;
       fi

    elif [ ${given_test} = 'NOT_EXISTS_CREATE' ]; then
        if test ! -d ${given_dir}
         then
           mkdir -p ${given_dir}
         fi
    else

       MSG=" Failed : provided option  ${given_test} on ${given_type} is not valid in function. Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1;
    fi


  ### check for HDFS type  ###
  elif [ ${given_type} = 'HDFS_DIR' ]; then

   if [ ${given_test} = 'VALIDATE'  ]; then

       hdfs dfs -test -d ${given_dir}
       if [ $? != 0 ]; then
               MSG=" Failed : provided Dir ${given_dir} on ${given_type} does not exists in HDFS. Terminating"
           msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
           exit 1;

       fi
   elif [ ${given_test} = 'NOT_EXISTS_CREATE' ]; then
        hdfs dfs -test -d ${given_dir}
        if [ $? != 0 ]; then
           hdfs dfs -mkdir -p ${given_dir}
         fi
    else

           MSG="Failed : provided option  ${given_test} on ${given_type} is not valid in function. Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1;
    fi


  else

        MSG=" Failed: provided Dir check system type ${given_type} is not a valid one. Terminating"
    msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
    exit 1;
  fi

  echo "End check_dir_exists function " >>  ${log}
}

######################################### do you like to Email or LOG messages #########################################
msg_email_or_log()
{


echo "Start msg_email_or_log function " >>  ${log}

EMAIL_SUB="CDH_DEV_${SCRIPT_NAME}_failed_@_${DATETIME}"
#EMAIL_ID="IT-DATALAKE-DEV@centurylink.com"
EMAIL_ID="Sireesha.Ravi@centurylink.com"


option=$1
msg=$2
msg_type=$3
msg=${msg}" please check log file ${log}  "

if [ ${option} = "LOG" ]; then
   echo "               " ${msg} >> ${log}
elif [ ${option} = "MAIL" ]; then
   echo "               "${msg} >> ${log}
   if [ ${msg_type} != "SUCCESS" ]; then
     echo "${msg}" | mail -s ${EMAIL_SUB} ${EMAIL_ID}
   fi
else
   echo "               " ${msg} >> ${log}
   echo ${msg}
fi

echo "End msg_email_or_log function " >>  ${log}
}




######################################### Loop through the landing zone and move files to staging and unzip  ###################################################
move_to_statging_from_landing()
{

echo "Start  move_to_statging_from_landing function " >>  ${log}

   lcl_file_name=${1}
   lcl_landing_dir=${2}
   lcl_staging_dir=${3}
   lcl_email_log_option=${4}

     echo "                     Begin processing file name: ${lcl_file_name}" >> ${log}
     mv ${lcl_landing_dir}/${lcl_file_name}* ${staging_dir}
     if [ $? -eq 0 ]
      then

        MSG="mv ${lcl_landing_dir}/${lcl_file_name}* ${lcl_staging_dir} completed successfully"
        msg_email_or_log ${lcl_email_log_option} "${MSG}" SUCCESS

     else
        MSG="mv ${lcl_landing_dir}/${lcl_file_name}* ${lcl_staging_dir} failed with non zero return code. Terminating"
        msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
        exit 1
     fi

echo "End  move_to_statging_from_landing function " >>  ${log}
}


######################### Combining the DAT files into a bigger file   ############################################################

merge_files()
{

 echo "Start merge_files function " >>  ${log}

   lcl_staging_dir=${1}
   lcl_email_log_option=${2}
   lcl_concat_file=${3}


 cat ${lcl_staging_dir}/* > ${lcl_staging_dir}/${lcl_concat_file}
 if [ $? -ne 0 ]
  then
     MSG="cat ${lcl_staging_dir}/* > ${lcl_staging_dir}/${lcl_concat_file} failed. Non zero returned code"
     msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
     exit 1
  fi
 echo "                 cat ${staging_dir}/* > ${staging_dir}/${lcl_concat_file} complete" >> ${log}


 echo "End merge_files function " >>  ${log}
}



####################### Create the HDFS partition directory ###########################################################
create_hdfs_partition()
{

echo "Start create_hdfs_partition function " >>  ${log}

   lcl_email_log_option=${1}
   lcl_hdfs_dir_to_create_hdfs_partition=${2}
   # echo " command to execute
   #       hdfs dfs -mkdir -p ${lcl_hdfs_dir_to_create_hdfs_partition} " >> ${log}

   hdfs dfs -mkdir -p ${lcl_hdfs_dir_to_create_hdfs_partition}
   if [ $? -ne 0 ]
    then
      MSG="hdfs dfs -mkdir -p ${lcl_hdfs_dir_to_create_hdfs_partition} failed. Non zero returned code"
      msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
      exit 1
   fi
   echo "               hdfs dfs -mkdir -p ${lcl_hdfs_dir_to_create_hdfs_partition} complete" >> ${log}

echo "End create_hdfs_partition function " >>  ${log}
}




####################### Ingest the file into HDFS ##########################################################
move_file_to_hdfs()
{

echo "Start move_file_to_hdfs function " >>  ${log}

   lcl_staging_dir=${1}
   lcl_email_log_option=${2}
   lcl_hdfs_dir_to_move_file_to_hdfs=${3}
   lcl_concat_file=${4}

   hdfs dfs -put ${lcl_staging_dir}/${lcl_concat_file} ${lcl_hdfs_dir_to_move_file_to_hdfs}
   if [ $? -ne 0 ]
    then
       MSG="hdfs dfs -put ${lcl_staging_dir}/${lcl_concat_file} ${lcl_hdfs_dir_to_move_file_to_hdfs} failed. Non zero returned code"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
    exit 1
   fi
  echo "                hdfs dfs -put ${lcl_staging_dir}/${lcl_concat_file} ${lcl_hdfs_dir_to_move_file_to_hdfs} complete" >> ${log}

echo "End move_file_to_hdfs function " >>  ${log}
}


###################### Remove all local files from staging directory #######################################
remove_files_from_staging_dir()
{

echo "Start remove_files_from_staging_dir function " >>  ${log}

   lcl_staging_dir=${1}
   lcl_email_log_option=${2}

   rm -f ${lcl_staging_dir}/*
   if [ $? -ne 0 ]
    then
       MSG="rm -f ${lcl_staging_dir}/* failed. Non zero returned code"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
    exit 1
   fi
   echo "               rm -f ${lcl_staging_dir}/* complete" >> ${log}

echo "End remove_files_from_staging_dir function " >>  ${log}

}


################################################## This Section prevents duplicate execution of the script ####################################################
check_if_a_process_exists()
{

echo "Start check_if_a_process_exists function " >>  ${log}

  lcl_pid_dir=${1}
  lcl_email_log_option=${2}
  lcl_script=${3}

  PIDFILE=${lcl_pid_dir}/${lcl_script}.pid
  ##### this echo will be used as a return value to the calling function ###
  echo ${PIDFILE}
  if [ -f ${PIDFILE} ]
  then
     PID=$(cat ${PIDFILE})
     ps -p $PID > /dev/null 2>&1
     if [ $? -eq 0 ]
     then

       MSG="${lcl_script} already executing with process ID - ${PID} .process ID found using ps -p. Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1

     else

       MSG="${lcl_script} already executing with process ID - ${PID} .But process ID not found using ps -p. something failed please move the process by fixing manually. Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1
     fi
 else
     echo $$ > ${PIDFILE}
     echo "                     Process ID" >> ${log}
     echo "                     "$$ >> ${log}
     if [ $? -ne 0 ]
      then

        MSG="${lcl_script} unable to create process ID lock file. Terminating"
        msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
        exit 1
     fi
 fi

echo "End check_if_a_process_exists function " >>  ${log}
}





################################################## This section decrypts the  file based of key ###########################################################

decrypt_file()
{

echo "Start decrypt_file function " >>  ${log}
   lcl_staging_dir=${1}
   lcl_decrypt_key=${2}
   lcl_email_log_option=${3}
   lcl_out_decrypt_file_dir=${4}
   lcl_encrypted_file=${5}

   # remove the a file if it already exists at the target decrypt dir  location
   rm -rf ${lcl_out_decrypt_file_dir}/${lcl_encrypted_file}

   echo "               decrypting file --->  ${lcl_staging_dir}/${lcl_encrypted_file} " >> ${log}
   gpg -d --passphrase ${lcl_decrypt_key}  --batch -o ${lcl_out_decrypt_file_dir}/${lcl_encrypted_file}   ${lcl_staging_dir}/${lcl_encrypted_file} &>>${log}

   if [ $? -ne 0 ]
     then

       MSG="${lcl_encrypted_file} is not able to decrypt. Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1
   fi


echo "End decrypt_file function " >>  ${log}

}


################################################## This section changes name of the file to a readable standard ######################################################


map_file_name_to_ctl_standard()
{
echo "Start map_file_name_to_ctl_standard function " >>  ${log}

  lcl_staging_dir={1}
  lcl_email_log_option=${2}
  lcl_input_file_name=${3}


  # check for which business unit this pdf letter is for like consumer or small_business(smb)
  pdf_for=`echo ${lcl_input_file_name} | awk -F '.' '{print toupper($4) }'`

  # here  we are finding the number from the file and trimming any spaces for the number in the file name
  # posible values are 03, 01, 02, 04
  #consumer_map_arry=("03:English_print_letters" "01:English_email_letters" "04:Spanish_print_letters" "02:Spanish_email_letters")
  #smb_map_arry=("02:English_print_letters" "01:English_email_letters" )

  pdf_type=`echo ${lcl_input_file_name} | awk -F '.' '{print toupper($5)}' | awk -F '_' '{print substr($1,length($1)-1)}' |  awk '{$1=$1};1'`


  # extract date value from file
  # example date -d "20180118" '+%m-%d-%Y'
  pdf_date=`echo ${lcl_input_file_name} | awk -F '.' '{print toupper($5)}' | awk -F '_' '{print toupper($2)}'`
  pdf_date_converted=`date -d "${pdf_date}" '+%m-%d-%Y'`

  if [ ${pdf_for} = "CONS" ]; then


        if [ ${pdf_type} =  "01" ]; then

            new_file_name="ENS_Consumer_English_email_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "02" ]; then

            new_file_name="ENS_Consumer_Spanish_email_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "03" ]; then

            new_file_name="ENS_Ensemble_English_print_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "04" ]; then

            new_file_name="ENS_Consumer_Spanish_print_letters_${pdf_date_converted}.pdf"

        else

          MSG="File name ${lcl_input_file_name} does not have valid number like 01, 02, 03 ,04  to identify if consumer file is English/Spanish email/print letter . Terminating"
          msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
          exit 1

        fi

  elif [ ${pdf_for} = "CONT" ]; then


        if [ ${pdf_type} =  "01" ]; then

            new_file_name="CRIS_Consumer_English_email_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "02" ]; then

            new_file_name="CRIS_Consumer_Spanish_email_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "03" ]; then

            new_file_name="CRIS_Consumer_English_print_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "04" ]; then

            new_file_name="CRIS_Consumer_Spanish_print_letters_${pdf_date_converted}.pdf"

        else

          MSG="File name ${lcl_input_file_name} does not have valid number like 01, 02, 03 ,04  to identify if consumer file is English/Spanish email/print letter . Terminating"
          msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
          exit 1

        fi

  elif [ ${pdf_for} = "SMBS" ]; then

        if [ ${pdf_type} =  "01" ]; then

            new_file_name="ENS_Business_English_email_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "02" ]; then

            new_file_name="ENS_Business_English_print_letters_${pdf_date_converted}.pdf"

        else

          MSG="File name ${lcl_input_file_name} does not have valid number like 01, 02  to identify if Business file is English email/print letter . Terminating"
          msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
          exit 1

        fi

  elif [ ${pdf_for} = "SMBT" ]; then

        if [ ${pdf_type} =  "01" ]; then

            new_file_name="CRIS_Business_English_email_letters_${pdf_date_converted}.pdf"

        elif [ ${pdf_type} =  "02" ]; then

            new_file_name="CRIS_Business_English_print_letters_${pdf_date_converted}.pdf"

        else

          MSG="File name ${lcl_input_file_name} does not have valid number like 01, 02  to identify if Business file is English email/print letter . Terminating"
          msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
          exit 1

        fi


  else

      MSG="File name ${lcl_input_file_name} does not have cons or smbs to identify if it is for consumer or small business . Terminating"
      msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
      exit 1
  fi

  #echo " given name is --  ${lcl_input_file_name} "
  echo  ${new_file_name}

echo "End map_file_name_to_ctl_standard function " >>  ${log}
}


############################################## This section parses the PDF file to text file  and extract customer information to a csv file##########################################

parse_pdf_file()
{

echo "Start parse_pdf_file function " >>  ${log}
  lcl_staging_dir=${1}
  lcl_email_log_option=${2}
  lcl_input_file_name=${3}
  lcl_python_code_dir=${4}

  if [ ${lcl_input_file_name} = '' ]; then
      MSG="File name ${lcl_input_file_name} is null, canot process pdf parsing . Terminating"
      msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
      exit 1
  fi

  pdf_file_name=${lcl_input_file_name}

  trimed_file_name=`basename ${pdf_file_name} | awk -F . '{print $1}'`
  actual_pdf_file_name=`basename ${pdf_file_name}`
  ${lcl_python_code_dir}/pdf2txt.py -A -F +1.0  -M 100 -L 0.5 ${lcl_staging_dir}/${pdf_file_name} > ${lcl_staging_dir}/${trimed_file_name}_pdf_parsed
  ${lcl_python_code_dir}/file_content_extractor_color.py ${actual_pdf_file_name} ${lcl_staging_dir}/${trimed_file_name}_pdf_parsed ${lcl_staging_dir}/${trimed_file_name}.csv

  #remove the header tags/columns from the csv file to be loaded to SOLR
  ex -sc '1d2|x' "${lcl_staging_dir}/${trimed_file_name}.csv"

  #add consumer or business column as the first into the csv file.
  unit_type=`echo ${trimed_file_name} | awk -F '_' '{print $1"_"$2}'`
  awk -v inp_unit_type=${unit_type} -F ',' '{print inp_unit_type","$0}' ${lcl_staging_dir}/${trimed_file_name}.csv > ${lcl_staging_dir}/${trimed_file_name}.csv_tt
  mv ${lcl_staging_dir}/${trimed_file_name}.csv_tt ${lcl_staging_dir}/${trimed_file_name}.csv

echo "End parse_pdf_file function " >>  ${log}

}





############################################## This section creates the SOLAR Index ########################################################

create_or_update_SOLR_index()
{
echo "Start create_or_update_SOLR_index function " >>  ${log}

  lcl_staging_dir=${1}
  lcl_email_log_option=${2}
  lcl_given_hdfs_dir=${3}
  lcl_solr_code_dir=${4}
  lcl_solr_idx_name=${5}
  lcl_solr_zookeeper_ips=${6}

  SOLR_CODE_DIR=${lcl_solr_code_dir}
  SOLR_DATA_HDFS_DIR="${lcl_given_hdfs_dir}/cust_letter_files/parsed/all_year_csv_ENS/"
  SOLR_MORPH_LINES_CONF_DIR=${lcl_solr_code_dir}
  SOLR_TEMP_DIR=${lcl_solr_code_dir}

  kinit -kt /home/cdlapp/cdlapp.keytab cdlapp@CTL.INTRANET


  tbl_name="${lcl_solr_idx_name}"


  rm -rf ${SOLR_CODE_DIR}/${tbl_name}_solr_collection

  ## create generated folder for each table solr schema
  solrctl instancedir --generate ${SOLR_CODE_DIR}/${tbl_name}_solr_collection


  cp ${SOLR_TEMP_DIR}/template_schema.xml ${SOLR_CODE_DIR}/${tbl_name}_solr_collection/conf/schema.xml

  sed -e "s/#####REPLACE_COLLECTION_NAME#######/${tbl_name}_solr_collection/g" ${SOLR_TEMP_DIR}/template_morph_lines.conf > ${SOLR_MORPH_LINES_CONF_DIR}/${tbl_name}_morph_lines.conf

  # check if the SOLR collection exists then delete
  existing_collection_name=`solrctl --zk ${lcl_solr_zookeeper_ips}/solr  collection --list |grep -i ${tbl_name}_solr_collection | awk '{print $1}'`
    # check if the name is non zero size
    if [ ! -z "${existing_collection_name}" ]; then
       # delete collection before creating one
           echo "Deleting the SOLR Collection ${tbl_name}_solr_collection " >> ${log}
       solrctl --zk ${lcl_solr_zookeeper_ips}/solr  collection --delete ${tbl_name}_solr_collection >> ${log} 2>&1
           if [ $? -ne 0 ]
         then

            MSG="SOLR Collection ${tbl_name}_solr_collection is not able to be deleted. Terminating"
            msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
            exit 1
       fi
    fi

  # check if the SOLR instance dir exists then delete
  existing_instancedir_name=`solrctl --zk ${lcl_solr_zookeeper_ips}/solr  instancedir --list |grep -i ${tbl_name}_solr_collection | awk '{print $1}'`
  # check if the name is non zero size
    if [ ! -z "${existing_instancedir_name}" ]; then
       # delete collection before creating one
           echo "Deleting the SOLR instance DIR ${tbl_name}_solr_collection " >> ${log}
       solrctl --zk ${lcl_solr_zookeeper_ips}/solr  instancedir --delete ${tbl_name}_solr_collection >> ${log} 2>&1
       if [ $? -ne 0 ]
         then

            MSG="SOLR instance DIR ${tbl_name}_solr_collection is not able to be deleted . Terminating"
            msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
            exit 1
       fi
    fi

  #
  ## create SOLR instancedir
  echo "Creating the SOLR instance DIR ${tbl_name}_solr_collection " >> ${log}
  solrctl --zk ${lcl_solr_zookeeper_ips}/solr  instancedir --create ${tbl_name}_solr_collection ${SOLR_CODE_DIR}/${tbl_name}_solr_collection >> ${log} 2>&1
  if [ $? -ne 0 ]
       then

       MSG="SOLR instance DIR ${tbl_name}_solr_collection is not able to be created . Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1
  fi

  #
  ## create SOLR collection
  echo "Creating the SOLR Collection DIR ${tbl_name}_solr_collection " >> ${log}
  solrctl --zk ${lcl_solr_zookeeper_ips}/solr  collection --create ${tbl_name}_solr_collection -s 7 -r 2 >> ${log} 2>&1
  if [ $? -ne 0 ]
       then

       MSG="SOLR Collection DIR ${tbl_name}_solr_collection is not able to be created . Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1
  fi

  #### updating the collection ##
  #  solrctl instancedir --update collection1 /path/to/collection1
  #  solrctl collection --reload collection

  #create hdfs if not exists for SOLR Index
  #create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_given_hdfs_dir}/cust_letter_files/solr_index/${tbl_name}_index/"
  create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_given_hdfs_dir}/cust_letter_files/solr_index/"

  hdfs dfs -rm -r -skipTrash "${lcl_given_hdfs_dir}/cust_letter_files/solr_index/${tbl_name}_index"
  echo "Loading the Data to SOLR Index  ${tbl_name}_index "
  echo "lcl_given_hdfs_dir ---->  ${lcl_given_hdfs_dir} "


  echo "Loading the Data to SOLR Index  ${tbl_name}_index " >> ${log}
  HADOOP_OPTS="-Djava.security.auth.login.config=${SOLR_CODE_DIR}/jaas.conf" hadoop jar /opt/cloudera/parcels/CDH/lib/solr/contrib/mr/search-mr-*-job.jar \
       org.apache.solr.hadoop.MapReduceIndexerTool \
     --zk-host ${lcl_solr_zookeeper_ips}/solr \
     --collection ${tbl_name}_solr_collection  \
     --morphline-file   ${SOLR_MORPH_LINES_CONF_DIR}/${tbl_name}_morph_lines.conf  \
     --output-dir hdfs://nameservice1/${lcl_given_hdfs_dir}/cust_letter_files/solr_index/${tbl_name}_index/  \
     --go-live  hdfs://nameservice1/${SOLR_DATA_HDFS_DIR}/   >> ${log} 2>&1

  if [ $? -ne 0 ]
       then

       MSG="SOLR Error in loading the data to Index . Terminating"
       msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
       exit 1
  fi
echo "End create_or_update_SOLR_index function " >>  ${log}

}





############################################## This section moves the data to HDFS ###########################################################

move_rrdonley_files_to_HDFS()
{

echo "Start move_rrdonley_files_to_HDFS function " >>  ${log}
  lcl_staging_dir=${1}
  lcl_email_log_option=${2}
  lcl_lang_type=${3}
  lcl_input_file_type=${4}
  lcl_hdfs_dir=${5}
  lcl_input_file_name=${6}

  if [ ${lcl_input_file_name} = '' ]; then
      MSG="File name ${lcl_input_file_name} is null, canot move processed files to hdfs . Terminating"
      msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
      exit 1
  fi

  pdf_file_name=${lcl_input_file_name}

  trimed_file_name=`basename ${pdf_file_name} | awk -F . '{print $1}'`
  actual_pdf_file_name=`basename ${pdf_file_name}`
  unit_type=`echo ${trimed_file_name} | awk -F '_' '{print $2}'`

  # extract Year value form pdf file_name date
  # example 01-26-2018 to 2018
  pdf_date=`echo ${trimed_file_name} | awk -F '_' '{print toupper($6)}'`
  pdf_date_year=`echo ${pdf_date} | awk -F '-' '{print $3}'`


  if [ ${lcl_input_file_type} = 'PDF' ]; then

      #create hdfs if not exists for storing original pdf files (both english and spanish)
          if [ ${lcl_lang_type} = 'ENGLISH' ]; then
         create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_hdfs_dir}/cust_letter_files/Original_pdf_files/${unit_type}/${pdf_date_year}/"
         move_file_to_hdfs ${lcl_staging_dir} ${lcl_email_log_option} "${lcl_hdfs_dir}/cust_letter_files/Original_pdf_files/${unit_type}/${pdf_date_year}/"  ${trimed_file_name}.pdf

                 #create hdfs if not exists for storing english pdf files only that are used for parsing
         create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_hdfs_dir}/cust_letter_files/English_pdfs/${unit_type}/${pdf_date_year}/"
         move_file_to_hdfs ${lcl_staging_dir} ${lcl_email_log_option} "${lcl_hdfs_dir}/cust_letter_files/English_pdfs/${unit_type}/${pdf_date_year}/"  ${trimed_file_name}.pdf
      fi

          if [ ${lcl_lang_type} = 'SPANISH' ]; then
             create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_hdfs_dir}/cust_letter_files/Original_pdf_files/${unit_type}/${pdf_date_year}/"
         move_file_to_hdfs ${lcl_staging_dir} ${lcl_email_log_option} "${lcl_hdfs_dir}/cust_letter_files/Original_pdf_files/${unit_type}/${pdf_date_year}/"  ${trimed_file_name}.pdf
      fi

  elif [ ${lcl_input_file_type} = 'PARSED' ]; then

       #create hdfs if not exists for storing parsed text files
       create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_hdfs_dir}/cust_letter_files/parsed/${unit_type}/${pdf_date_year}/color/text_files"
       move_file_to_hdfs ${lcl_staging_dir} ${lcl_email_log_option} "${lcl_hdfs_dir}/cust_letter_files/parsed/${unit_type}/${pdf_date_year}/color/text_files"    ${trimed_file_name}_pdf_parsed

  elif [ ${lcl_input_file_type} = 'CSV' ]; then

       #create hdfs if not exists for storing csv files
       create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_hdfs_dir}/cust_letter_files/parsed/${unit_type}/${pdf_date_year}/color/csv_files/"
       move_file_to_hdfs ${lcl_staging_dir} ${lcl_email_log_option} "${lcl_hdfs_dir}/cust_letter_files/parsed/${unit_type}/${pdf_date_year}/color/csv_files/"   ${trimed_file_name}.csv

       #create hdfs if not exists for storing all csv files for creating SOLR Index
       create_hdfs_partition ${EMAIL_LOG_OPTION} "${lcl_hdfs_dir}/cust_letter_files/parsed/all_year_csv_ENS/"
       move_file_to_hdfs ${lcl_staging_dir} ${lcl_email_log_option} "${lcl_hdfs_dir}/cust_letter_files/parsed/all_year_csv_ENS/"   ${trimed_file_name}.csv


  else
      MSG="Canot move the file to HDFS, as pecified type is not valid for RRD storage in Hadoop ( check function move_rrdonley_files_to_HDFS() ) . Terminating"
      msg_email_or_log ${lcl_email_log_option} "${MSG}" FAILED
      exit 1
  fi


echo "End move_rrdonley_files_to_HDFS function " >>  ${log}

}







############################################## This section controls all  logic ########################################################

if [[ "$#" -ne 4 ]]; then
    usage
    exit 1
else



   SCRIPT_NAME=`basename "$0"`

   SFTP_HOME_DIR=/data/CTL/ingest
   SFTP_INGEST_SOURCE=$1
   SFTP_INGEST_DIR=$2
   HDFS_LOC=$3
   EMAIL_LOG_OPTION=$4


   DATETIME=`date '+%y%m%d_%H%M%S'`
   DATE=`date '+%Y%m%d'`

   decrypt_key=DonnelyRR17

   pid_dir=${SFTP_HOME_DIR}/${SFTP_INGEST_SOURCE}/script
   log_dir=${SFTP_HOME_DIR}/${SFTP_INGEST_SOURCE}/log
   staging_dir=${SFTP_HOME_DIR}/${SFTP_INGEST_SOURCE}/staging
   landing_dir=${SFTP_INGEST_DIR}
   decrypt_dir=${SFTP_HOME_DIR}/${SFTP_INGEST_SOURCE}/decrypted


   concat_file=${SFTP_INGEST_SOURCE}_${DATETIME}.dat

   log=${log_dir}/${SCRIPT_NAME}_${DATETIME}.log


   ####### validate the dir exists or not and create if needed ####
   check_dir_exists ${SFTP_HOME_DIR} VALIDATE  UNIX_DIR  ${EMAIL_LOG_OPTION}
   check_dir_exists ${SFTP_INGEST_DIR} VALIDATE  UNIX_DIR ${EMAIL_LOG_OPTION}
   check_dir_exists ${pid_dir} NOT_EXISTS_CREATE UNIX_DIR ${EMAIL_LOG_OPTION}
   check_dir_exists ${log_dir} NOT_EXISTS_CREATE  UNIX_DIR ${EMAIL_LOG_OPTION}
   check_dir_exists ${staging_dir} NOT_EXISTS_CREATE UNIX_DIR ${EMAIL_LOG_OPTION}
   check_dir_exists ${decrypt_dir} NOT_EXISTS_CREATE UNIX_DIR ${EMAIL_LOG_OPTION}
   check_dir_exists ${landing_dir} NOT_EXISTS_CREATE UNIX_DIR ${EMAIL_LOG_OPTION}

   #given parent hdfs dir location where customer letter will go
   inp_hdfs_dir=${HDFS_LOC}

   ###### check if HDFS dir exists or not ###
   check_dir_exists ${inp_hdfs_dir} NOT_EXISTS_CREATE HDFS_DIR  ${EMAIL_LOG_OPTION}


   ## some key variable values that are needed for the process
   solr_zookeeper_ips="polpcdhmn001.corp.intranet:2181,polpcdhmn002.corp.intranet:2181,polpcdhmn003.corp.intranet:2181"
   PYTHON_CODE_DIR='/home/cdlapp/ingestion/ens_mkt_welcome_letters/python_code'
   SOLR_CODE_DIR='/home/cdlapp/ingestion/ens_mkt_welcome_letters/solr_code'
   SOLR_INDX_NAME='pdf_cust_letter_ens'


   ### test code only
   #cp ${landing_dir}/source_files/*.pdf ${landing_dir}



fi



echo "Starting execution of ${SCRIPT_NAME} @ ${DATETIME}" >> ${log}
echo "SFTP_HOME_DIR ${SFTP_HOME_DIR} "  >> ${log}
echo "SFTP_INGEST_SOURCE ${SFTP_INGEST_SOURCE}" >> ${log}
echo "SFTP_INGEST_DIR ${SFTP_INGEST_DIR} " >> ${log}
echo "HDFS_LOC ${HDFS_LOC} " >> ${log}
echo "pid_dir ${pid_dir} " >> ${log}
echo "log_dir ${log_dir} "  >> ${log}
echo "staging_dir ${staging_dir} " >> ${log}
echo "decrypt_dir ${decrypt_dir} " >> ${log}
echo "landing_dir ${landing_dir} " >> ${log}
echo "hdfs_dir ${hdfs_dir} " >> ${log}
echo "concat_file ${concat_file} " >> ${log}
echo "log ${log} " >> ${log}



 ### we are calling the check process and getting the return value as a subshell #####
 RTN_PIDFILE=$(check_if_a_process_exists ${pid_dir} ${EMAIL_LOG_OPTION} ${SCRIPT_NAME})

 ### as the above call is a subprocess the exit status from inside the function is not captures for the ######
 ### this main process, so any errors we capture and exit ######
 if [ $? -ne 0 ]
      then
      exit 1;
 fi
 #echo "status $? "
 #echo "RTN_PIDFILE ${RTN_PIDFILE} "

 # are there any files to process, if not exit

 num_pdf_files=`ls -lrt ${landing_dir}/*.pdf | wc -l`

 if [ ${num_pdf_files} -eq 0 ]
  then
  echo " no files to process "
  rm -f ${RTN_PIDFILE}
  exit 1;
 fi


for file_name in `ls -lrt ${landing_dir}/*.pdf* | awk '{print $9}' `
do

 move_to_statging_from_landing `basename ${file_name}`  ${landing_dir} ${staging_dir} ${EMAIL_LOG_OPTION}
# decrypt_file ${staging_dir}  ${decrypt_key} ${EMAIL_LOG_OPTION}   ${decrypt_dir}  `basename ${file_name}`
cp /data/CTL/ingest/ensmktltr/staging/* /data/CTL/ingest/ensmktltr/decrypted/
 RTN_New_FileName=$(map_file_name_to_ctl_standard ${decrypt_dir}  ${EMAIL_LOG_OPTION} `basename ${file_name}`)

 mv ${decrypt_dir}/`basename ${file_name}`  ${staging_dir}/${RTN_New_FileName}

 # we check to see if the pdf file is spanish one,
 # if it is , then we dont have to parse it.
 spanish_lang_string='Spanish'
 #handling english files
 if [[ ${RTN_New_FileName} != *${spanish_lang_string}* ]]; then
   echo "               parsing file --->  ${RTN_New_FileName} "  >> ${log}
   parse_pdf_file ${staging_dir} ${EMAIL_LOG_OPTION} ${RTN_New_FileName} "${PYTHON_CODE_DIR}"
   if [ $? -ne 0 ]
     then

       MSG="Error in parsing file ${RTN_New_FileName}. Terminating."
       msg_email_or_log ${EMAIL_LOG_OPTION} "${MSG}" FAILED
       exit 1

   fi

   echo "               finished parsing file --->  ${RTN_New_FileName} "  >> ${log}

   move_rrdonley_files_to_HDFS ${staging_dir} ${EMAIL_LOG_OPTION} "ENGLISH" "PDF" ${inp_hdfs_dir} ${RTN_New_FileName}
   move_rrdonley_files_to_HDFS ${staging_dir} ${EMAIL_LOG_OPTION} "ENGLISH" "PARSED" ${inp_hdfs_dir} ${RTN_New_FileName}
   move_rrdonley_files_to_HDFS ${staging_dir} ${EMAIL_LOG_OPTION} "ENGLISH" "CSV" ${inp_hdfs_dir} ${RTN_New_FileName}

 #handling spanish files

 else
   move_rrdonley_files_to_HDFS ${staging_dir} ${EMAIL_LOG_OPTION} "SPANISH" "PDF" ${inp_hdfs_dir} ${RTN_New_FileName}
 fi

done


 create_or_update_SOLR_index ${staging_dir} ${EMAIL_LOG_OPTION} "${inp_hdfs_dir}" "${SOLR_CODE_DIR}" "${SOLR_INDX_NAME}" "${solr_zookeeper_ips}"

 remove_files_from_staging_dir ${staging_dir} ${EMAIL_LOG_OPTION}

 rm -f ${RTN_PIDFILE}


 #### test code only
 #hdfs dfs -rm -r -skipTrash ${HDFS_LOC}/*

 #map_file_name_to_ctl_standard "test" ${EMAIL_LOG_OPTION} "chgp.fin.ctlXXXX.cons.outpdf05_20180228"
