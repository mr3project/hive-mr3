#!/bin/bash

# Ex. 
#   git diff f4e0529634b6231a0072295da48af466cf2f10b7 bcc7df95824831a8d2f1524e4048dfc23ab98c19 > diff.diff
#   csplit -z -f 'patch' -b '%02d.diff' diff.diff /^diff/ {*}

function apply {
  declare file=$1

  if [[ $(git apply $file 2>&1 | grep "error: patch failed") ]]; then
    echo "$file failed"
  else
    echo "$file applied okay"
    rm $file 
  fi 
}

for file in $(eval ls patch*.diff); do
  if [[ $(head -n 1 $file | grep "diff .*q\.out .*q\.out") ]]; then
    apply $file
  fi 
done

for file in $(eval ls patch*.diff); do
  if [[ $(head -n 1 $file | grep "diff .*q\.out_spark .*q\.out_spark") ]]; then
    apply $file
  fi 
done

for file in $(eval ls patch*.diff); do
  if [[ $(head -n 1 $file | grep "diff .*\.q .*\.q") ]]; then
    apply $file
  fi 
done

for file in $(eval ls patch*.diff); do
  if [[ $(head -n 1 $file | grep "diff .*pom\.xml .*pom\.xml") ]]; then
    apply $file
  fi 
done

for file in $(eval ls patch*.diff); do
  if [[ $(head -n 1 $file | grep "diff .*RELEASE_NOTES\.txt .*RELEASE_NOTES\.txt") ]]; then
    apply $file
  fi 
done

for file in $(eval ls patch*.diff); do
  if [[ $(head -n 1 $file | grep "diff .*\.java .*\.java") ]]; then
    apply $file
  fi 
done

