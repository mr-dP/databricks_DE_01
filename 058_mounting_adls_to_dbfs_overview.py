# Databricks notebook source
# Databricks mounts create a link between a Workspace and a cloud object storage
# This enables you to interact with the cloud object storage using familiar file paths relative to the Databricks File System
# Mounts work by creating a local alias under the "/mnt" directory that stores the location of the cloud object storage. It also stores the driver's specifications to connect to the Storage Account or Container and the security credentials required to access the data

# COMMAND ----------

# 1. We need to register an Azure AD Application to create a Service Principal
#       An Azure Service Principal is a security identity used to access specific Azure resources
#       It is similar to a user identity with a specific role such as a Contributor for a specific resource
#       The access is restricted by the roles assigned to it
#       When we create a Service Principal, we then get an Application Client ID, an Application Tenant ID and and a Client Secret

# 2. We then need to get the Service Principal to the ADLS account

# 3. Finally with the Application Client ID, the Tenant ID and the Secret we can mount the Storage to the Databricks File System
