#!/bin/bash

set -x

# should check data path in conf
# should check xgboost bin
eta_set=(0.01 0.1 0.5 1)
gamma_set=(0.01 0.1 0.5 1)
min_child_set=(0.1 0.5 1 5)
depth_set=(3 6)
template_conf="final_temp.conf"

function prepare_env() {
  for eta in ${eta_set[@]}; do
    for gamma in ${gamma_set[@]}; do
      for min_child_weight in ${min_child_set[@]}; do
        for depth in ${depth_set[@]}; do
          name="eta_${eta}_gamma_${gamma}_minchild_${min_child_weight}_depth_${depth}"
          rm -rf $name
          mkdir -p $name
          sed  -e "s/eta = 1.0/eta = $eta/" -e "s/gamma = 1.0/gamma = $gamma/" -e "s/min_child_weight = 1/min_child_weight = $min_child_weight/" -e "s/max_depth = 3/max_depth = $depth/" $template_conf > $name/task_conf
          cp xgboost $name
        done
      done
    done
  done
}

function runexp() {
  for eta in ${eta_set[@]}; do
    for gamma in ${gamma_set[@]}; do
      for min_child_weight in ${min_child_set[@]}; do
        for depth in ${depth_set[@]}; do
          name="eta_${eta}_gamma_${gamma}_minchild_${min_child_weight}_depth_${depth}"
          n_proc=$(ps aux  |grep xgboost | wc -l)
          while [ $n_proc -gt 10 ]; do
            sleep 5
          done
          cd  $name
          ./xgboost task_conf > out 2>&1 &
          cd -
        done
      done
    done
  done
}


if [ $1 == "single" ]; then
  ./xgboost $template_conf
elif [ $1 == "exp" ]; then
  prepare_env
  runexp
else
  echo "Usage: ./tune.sh [single | exp]"
fi
