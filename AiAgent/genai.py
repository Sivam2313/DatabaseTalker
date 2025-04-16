# pylint: disable=broad-exception-caught,invalid-name

import time

from google import genai
from google.cloud import bigquery
from google.genai.types import FunctionDeclaration, GenerateContentConfig, Part, Tool
import streamlit as st
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

MODEL_ID = "gemini-2.0-flash"
LOCATION = "us-central1"

sql_query_function = FunctionDeclaration(
    name="sql_query",
    description="Get information from data in postgres using SQL queries",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query using fully qualified table names.",
            }
        },
        "required": ["query"],
    },
)

mongo_query_function = FunctionDeclaration(
    name="mongo_query",
    description="Get information from MongoDB using find queries",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "object",
                "description": "Mongo find query using $expr for field comparisons.",
            }
        },
        "required": ["query"],
    },
)


sql_query_tool = Tool(
    function_declarations=[
        sql_query_function,
        mongo_query_function
    ],
)

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

st.set_page_config(
    page_title="SQL Talk with BigQuery",
    layout="wide",
)

col1, col2 = st.columns([8, 1])
with col1:
    st.title("Talk with Database")

st.subheader("Database query")

with st.expander("Sample prompts", expanded=True):
    st.write(
        """
        - What kind of information is in this database?
        - Get all customers from customer table in postgres.
        - Get all customers from customer table in postgres. and give me all those customers with the same id from mongodb
        - Get all customers from customer table in postgres and give me all those customers from mongodb with the same id field then Calculate the total loan where total loan = external_loans from mongodb data and loans from postgres and give me the highest total loan.
        """
    )

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"].replace("$", r"\$"))  # noqa: W605
        try:
            with st.expander("Function calls, parameters, and responses"):
                st.markdown(message["backend_details"])
        except KeyError:
            pass

if prompt := st.chat_input("Ask me about information in the database..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        chat = client.chats.create(
            model=MODEL_ID,
            config=GenerateContentConfig(temperature=0, tools=[sql_query_tool]),
        )

        prompt += """
            Please give a concise, high-level summary followed by detail in
            plain language about where the information in your response is
            coming from in the database. Only use information that you learn
            from the database, do not make up information.
            If you feel you need more information to do the operation tell that also.
            """

        try:
            response = chat.send_message(prompt)
            response = response.candidates[0].content.parts[0]

            # if hasattr(response, "text"):
            #     print(response)
            #     st.markdown(response.text)
            #     st.session_state.messages.append({
            #         "role": "assistant",
            #         "content": response.text
            #     })
            # print("1",response)

            api_requests_and_responses = []
            backend_details = ""

            function_calling_in_process = True
            while function_calling_in_process:
                try:

                    params = {}
                    for key, value in response.function_call.args.items():
                        params[key] = value

                    print(response.function_call.name)
                    print(params)

                    if response.function_call.name == "mongo_query":
                        url = "http://localhost:5000/query_mongo"
                        payload = params
                        try:
                            api_response = requests.post(url, json=payload)

                            if api_response.status_code == 200:
                                api_requests_and_responses.append(
                                    [response.function_call.name, params, api_response.json()]
                                )

                        except requests.exceptions.RequestException as e:
                            print(f"Request failed: {e}")

                    if response.function_call.name == "sql_query":
                        url = "http://localhost:5000/execute_sql"
                        payload = params
                        try:
                            api_response = requests.post(url, json=payload)
                            if api_response.status_code == 200:
                                api_requests_and_responses.append(
                                    [response.function_call.name, params, api_response.json()]
                                )

                        except requests.exceptions.RequestException as e:
                            print(f"Request failed: {e}")

                    # print(response.function_call.name,api_response.json())

                    # print("1",response.function_call.name)
                    response = chat.send_message(
                        Part.from_function_response(
                            name=response.function_call.name,
                            response={
                                "content": api_response.json(),
                            },
                        ),
                    )
                    response = response.candidates[0].content.parts[0]

                    # print(api_requests_and_responses)

                    backend_details += "- Function call:\n"
                    backend_details += (
                        "   - Function name: ```"
                        + str(api_requests_and_responses[-1][0])
                        + "```"
                    )
                    backend_details += "\n\n"
                    backend_details += (
                        "   - Function parameters: ```"
                        + str(api_requests_and_responses[-1][1])
                        + "```"
                    )
                    backend_details += "\n\n"
                    backend_details += (
                        "   - API response: ```"
                        + str(api_requests_and_responses[-1][2])
                        + "```"
                    )
                    backend_details += "\n\n"
                    with message_placeholder.container():
                        st.markdown(backend_details)

                except AttributeError:
                    function_calling_in_process = False

            time.sleep(3)

            full_response = response.text
            with message_placeholder.container():
                st.markdown(full_response.replace("$", r"\$"))  # noqa: W605
                with st.expander("Function calls, parameters, and responses:"):
                    st.markdown(backend_details)

            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": full_response,
                    "backend_details": backend_details,
                }
            )
        except Exception as e:
            print(e)
            error_message = f"""
                Something went wrong! We encountered an unexpected error while
                trying to process your request. Please try rephrasing your
                question. Details:

                {str(e)}"""
            st.error(error_message)
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": error_message,
                }
            )