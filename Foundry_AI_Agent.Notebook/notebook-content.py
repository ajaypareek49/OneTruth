# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install azure-identity
%pip install "azure-ai-projects>=2.1.0"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from azure.identity import DeviceCodeCredential

tenant_id = "baf5b083-4c53-493a-8af7-a6ae9812014c"

credential = DeviceCodeCredential(tenant_id=tenant_id)

token = credential.get_token("https://cognitiveservices.azure.com/.default")
print("Token Generated!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code


from azure.identity import ManagedIdentityCredential
from azure.ai.projects import AIProjectClient

import ipywidgets as widgets
from IPython.display import display

endpoint = "https://odl-user-2448917-5109-resource.services.ai.azure.com/api/projects/odl-user-2448917-5109"
agent_name = "OneTruth-logistics-Foundry-AI"


with (
    credential,
    AIProjectClient(
        endpoint=endpoint,
        credential=credential,
        allow_preview=True,
    ) as project_client,
):
    # Bind the OpenAI client to the existing agent endpoint.
    openai_client = project_client.get_openai_client(agent_name=agent_name)

    user_question = "What is our overall on-time delivery performance this year (2024)?"
    # GitHub Copilot preview agents require a conversation for every Responses turn.
    conversation = openai_client.conversations.create()
    response = openai_client.responses.create(
        conversation=conversation.id,
        input=[{"role": "user", "content": user_question}],
    )

    print(f"Response output: {response.output_text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
