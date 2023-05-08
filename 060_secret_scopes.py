# Databricks notebook source
# Azure Key Vault is a cloud service that provides a secure store for your secrets. You can securely store keys, passwords, certificates as well as other secrets

# COMMAND ----------

# In Databricks, you can create a Secret Scope. Essentially, this takes the secret values from the Azure Key Vault where you reference the key and the secret is returned but it is not displayed

# https://adb-****************.**.azuredatabricks.net/?o=****************#secrets/createScope

# Scope Name            => Name of the Azure Key Vault
# Managed Principal     => All Users                                            [This mean that all users can manage the Secret Scopes]
# DNS Name              => Azure Key Vault -> Properties -> Vault URI
# Resource ID           => Azure Key Vault -> Properties -> Resource ID

# These allows Databricks to communicate with Key Vaults to obtain the secret values

# COMMAND ----------

dbutils.secrets.help()

# Provides utilities for leveraging secrets within notebooks. Databricks documentation for more info.

# get(scope: String, key: String): String -> Gets the string representation of a secret value with scope and key

# getBytes(scope: String, key: String): byte[] -> Gets the bytes representation of a secret value with scope and key

# list(scope: String): Seq -> Lists secret metadata for secrets within a scope

# listScopes: Seq -> Lists secret scopes

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

display(dbutils.secrets.list("databricks-secrets-00x00"))

# COMMAND ----------

display(dbutils.secrets.getBytes("databricks-secrets-00x00", "application-id"))

# b'********-****-****-****-************'

# COMMAND ----------

display(dbutils.secrets.get("databricks-secrets-00x00", "application-id"))

# The result is redacted. So, you cannot actually see the value

# '[REDACTED]'

# COMMAND ----------

# However this does not mean that it is not possible to obtain it. you can do a simple for-loop and print the values

for i in dbutils.secrets.get("databricks-secrets-00x00", "application-id"):
    print(i)

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

application_id = dbutils.secrets.get(
    scope="databricks-secrets-00x00", key="application-id"
)
tenant_id = dbutils.secrets.get(scope="databricks-secrets-00x00", key="tenant-id")
secret = dbutils.secrets.get(scope="databricks-secrets-00x00", key="secret")

# COMMAND ----------

container_name = "bronze"
account_name = "00x00dlorders"
mount_point = "/mnt/bronze"

# COMMAND ----------

dbutils.fs.unmount(mount_point)

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source=f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs,
)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


