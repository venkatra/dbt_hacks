#!/bin/bash

###############################################################################
#
#   This script is used to specifically to deploy models using dbt. The models to deploy are found in the file
#   DeployableModels.txt
#
###############################################################################

. ./configs.sh

fn_deploy() {
  param_sql_script=$1

  echo "###############################################################################"
  echo "  Script : ${param_sql_script} "
  echo "###############################################################################"
  model_name=$(basename $param_sql_script | sed 's/.sql//g')

  dbt run -m $model_name
  execution_status=$? #Captures the status of execution of sql script

  echo "Status of execution : $execution_status"
}

input="./DeployableModels.txt"
while IFS= read -r line
do
  sql_script_file=$line
  fn_deploy $sql_script_file
done < "$input"