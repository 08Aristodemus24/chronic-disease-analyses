# run setup_infra.sh

# terrraform apply with argument -auto-approve just auto yes's the
# prompt apply changes. Note the way to refer to bash arguments
# is like variables where the variable is preceded by a $ char
if [ $1 == "setup" ]; then
    echo "Initiating infrastructure build"
    terraform init
    terraform fmt
    terraform apply -auto-approve

elif [ $1 == "destroy" ]; then
    echo "Initiating infrastructure destruction"
    terraform destroy -auto-approve

elif [ $1 == "plan" ]; then
    echo "Running plan before infrastructure build"
    terraform plan

else
    echo "No command executed. Please use 'setup' or 'destroy' as an argument."

fi