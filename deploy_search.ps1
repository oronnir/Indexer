# Variables
resourceGroup="myResourceGroup"
location="westus2"
serviceName="mySearchService"
sku="free" # Change this as per your requirements
containerGroupName="myContainerGroup"
containerImage="openai/clip" # Replace with the actual CLIP encoder image

# Create a resource group
az group create --name $resourceGroup --location $location

# Create Azure Cognitive Search service
az search service create --name $serviceName --resource-group $resourceGroup --sku $sku --location $location

# Create Azure Container Instance
az container create --name $containerGroupName --resource-group $resourceGroup --image $containerImage --dns-name-label $containerGroupName --ports 80