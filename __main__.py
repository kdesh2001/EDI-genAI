from neo4j import GraphDatabase
import json
import pprint
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import red, black, green
import pandas as pd

import ibm_boto3
from ibm_botocore.client import Config, ClientError


import requests

API_URL = "https://api-inference.huggingface.co/models/mistralai/Mixtral-8x7B-Instruct-v0.1"
headers = {"Authorization": "Bearer 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"}


def ask_mixtral(payload):
    response = requests.post(API_URL, headers=headers, json=payload)
    return response.json()


# Constants for IBM COS values
COS_ENDPOINT = "https://s3.us-south.cloud-object-storage.appdomain.cloud" # Current list avaiable at https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints
COS_API_KEY_ID = "9ITYiSk-BpydNpRFnwTczyliKE5VFqaTZEPtkhI_5MeH" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
COS_INSTANCE_CRN = "crn:v1:bluemix:public:iam-identity::a/010dcec46cce40a9a8555682c8c82e84::serviceid:ServiceId-f61fe320-d830-4f83-bb07-e31daa91878d" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::"

# Create resource
cos = ibm_boto3.resource("s3",
    ibm_api_key_id=COS_API_KEY_ID,
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)

'''
{
    "apikey": "9ITYiSk-BpydNpRFnwTczyliKE5VFqaTZEPtkhI_5MeH",
    "endpoints": "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints",
    "iam_apikey_description": "Auto-generated for key crn:v1:bluemix:public:cloud-object-storage:global:a/010dcec46cce40a9a8555682c8c82e84:73483270-b430-45f6-aec0-595bbac30668:resource-key:89a2eb67-d52c-40b4-8d45-1a26870d2942",
    "iam_apikey_name": "edi-cloud-func",
    "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
    "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/010dcec46cce40a9a8555682c8c82e84::serviceid:ServiceId-f61fe320-d830-4f83-bb07-e31daa91878d",
    "resource_instance_id": "crn:v1:bluemix:public:cloud-object-storage:global:a/010dcec46cce40a9a8555682c8c82e84:73483270-b430-45f6-aec0-595bbac30668::"
}
'''

def multi_part_upload(bucket_name, item_name, file_data):
    try:
        print("Starting file transfer for {0} to bucket: {1}\n".format(item_name, bucket_name))
        # set 5 MB chunks
        part_size = 1024 * 1024 * 5

        # set threadhold to 15 MB
        file_threshold = 1024 * 1024 * 15

        # set the transfer threshold and chunk size
        transfer_config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold=file_threshold,
            multipart_chunksize=part_size
        )

        # the upload_fileobj method will automatically execute a multi-part upload
        # in 5 MB chunks for all files over 15 MB
        # with open(file_path, "rb") as file_data:
        cos.Object(bucket_name, item_name).upload_fileobj(
            Fileobj=file_data,
            Config=transfer_config
        )

        print("Transfer for {0} Complete!\n".format(item_name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to complete multi-part upload: {0}".format(e))

# def create_word_doc_with_dict(data_dict):
#     doc = Document()
#
#     # Convert the dictionary to a pretty-printed JSON-formatted string
#     json_string = json.dumps(data_dict, indent=4)
#     pretty_json = pprint.pformat(json.loads(json_string))
#
#     # Write the JSON content to the Word document
#     doc.add_paragraph(pretty_json)
#
#     # Save the Word document
#     doc.save("output_new1.docx")


def create_pdf_with_dict(data_dict):
    buffer = BytesIO()  # Create a BytesIO buffer to hold the PDF content
    doc = SimpleDocTemplate(buffer, pagesize=letter)

    # Set up the styles for the text
    styles = getSampleStyleSheet()
    style_normal = styles['Normal']

    # Convert the dictionary to a pretty-printed JSON-formatted string
    json_string = json.dumps(data_dict, indent=4)
    pretty_json = pprint.pformat(json.loads(json_string))

    # Split the pretty-printed JSON string into lines
    lines = pretty_json.splitlines()

    # Create a list of Paragraph objects with appropriate line breaks
    text_content = [Paragraph(line, style_normal) for line in lines]

    # Build the PDF document
    doc.build(text_content)

    # Move the buffer's pointer back to the beginning
    buffer.seek(0)

    return buffer


def create_pdf(transaction_set, version, segments, elements):
    width, height = letter
    print(width, height)
    buffer = BytesIO()
    # Create a canvas object
    c = canvas.Canvas(buffer, pagesize=letter)

    c.setFont("Helvetica", 10)
    summary = get_summary(agencies_description[agencies.index(agency)], version, tset,
                          list(segments["SegmentID"].unique()))
    style = getSampleStyleSheet()['Normal']
    paragraph = Paragraph(summary, style)

    # Draw the paragraph on the canvas
    paragraph.wrapOn(c, inch * 6, inch * 6)
    paragraph.drawOn(c, inch, inch * 6)
    c.showPage()

    # TransactionSet and Version
    c.setFont("Helvetica-Bold", 40)
    c.drawString(20, 740, transaction_set)

    c.setFont("Helvetica-Bold", 20)
    c.setFillColor(red)
    c.drawString(20, 740-30, "VER." + version)

    # Heading line definition
    c.setLineWidth(2)
    c.line(150, 760, 550, 760)

    # Transaction Set Description
    c.setFillColor(black)
    c.setFont("Helvetica-Bold", 20)
    c.drawString(170, 736, "Purchase Order")
    c.setLineWidth(2)
    c.line(150, 730, 550, 730)

    c.setFont("Helvetica-Bold", 10)
    c.setLineWidth(2)
    c.drawString(30, 650, "Pos")
    c.drawString(90, 650, "ID")
    c.drawString(150, 650, "SegmentName")
    c.drawString(400, 650, "Req")
    c.drawString(430, 650, "MaxUsage")
    c.drawString(500, 650, "Repeat")
    c.drawString(550, 650, "Notes")
    c.setFont("Helvetica", 10)
    c.setLineWidth(1)
    for i, s in segments.iterrows():
        c.drawString(30, 650-(i+1)*20, s["Position"])
        c.drawString(90, 650-(i+1)*20, s["SegmentID"])
        c.drawString(150, 650-(i+1)*20, s["SegmentDescription"])
        c.drawString(400, 650-(i+1)*20, s["RequirementDesignator"])
        c.drawString(430, 650-(i+1)*20, s["MaximumUsage"])
        c.drawString(500, 650-(i+1)*20, s["LoopID"])
        # c.drawString(550, 650, s["Notes"])
    c.showPage()

    # For each segment
    for ii, s in segments.iterrows():
        c.setFont("Helvetica-Bold", 40)
        c.drawString(20, 740, s["SegmentID"])

        # Heading line
        c.setLineWidth(2)
        c.line(140, 760, 550, 760)

        # Segment Name/Description
        c.setFillColor(black)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(150, 736, s["SegmentDescription"])

        df = elements[elements["SegmentID"] == s["SegmentID"]].reset_index()

        # Rectangle
        c.line(460, 760, 580, 760)
        c.line(460, 760, 460, 710)
        c.line(460, 710, 580, 710)
        c.line(580, 760, 580, 710)
        c.setFont("Helvetica", 8)
        c.drawString(470, 742, "Pos: " + str(s["Position"]))
        c.drawString(520, 742, "Max: " + str(s["MaximumUsage"]))
        c.drawString(470, 720, "Loop: " + str(s["LoopID"]))
        c.drawString(520, 720, "Elements: " + str(df.shape[0]))
        c.drawString(492, 731, str(s["Section"] + "-" + str(s["RequirementDesignator"])))

        c.setFont("Helvetica-Bold", 10)
        c.setLineWidth(2)
        c.drawString(30, 650, "Ref")
        c.drawString(90, 650, "ElementID")
        c.drawString(150, 650, "ElementName")
        c.drawString(400, 650, "Req")
        c.drawString(430, 650, "Type")
        c.drawString(480, 650, "Min/Max")
        c.drawString(550, 650, "Notes")
        c.setFont("Helvetica", 10)
        c.setLineWidth(1)
        for i, e in df.iterrows():
            c.drawString(30, 650 - (i + 1) * 20, e["Ref"])
            c.drawString(90, 650 - (i + 1) * 20, e["ElementID"])
            c.drawString(150, 650 - (i + 1) * 20, e["Description"])
            c.drawString(400, 650 - (i + 1) * 20, e["RequirementDesignator"])
            c.drawString(430, 650 - (i + 1) * 20, e["Type"])
            c.drawString(480, 650 - (i + 1) * 20, e["MinimumLength"]+"/"+e["MaximumLength"])
            # c.drawString(550, 650 - (i + 1) * 20, e["Notes"])
        c.showPage()

    # Save the PDF
    c.save()
    buffer.seek(0)

    return buffer


neo4j_uri = 'neo4j+s://a2903c3d.databases.neo4j.io'
neo4j_user = 'neo4j'
neo4j_password = 'x7lO8GKrglcmm4MYuHcBp_PJx23STanbAUKfnuj_FIg'

# neo4j_uri = 'neo4j+s://6d01a70f.databases.neo4j.io'
# neo4j_user = 'neo4j'
# neo4j_password = 'qbAQwpVHaprehjHgapCW8zad2jMXeExqZ7GgpfW1tcE'


def get_summary(agency, version, tset,  segmentIDs):
    question = f'''
    You are an expert in EDI. You have made an EDI spec document for a customer. Key details of that specifications are: {agency}, version {version}, transaction set is {tset}, segments are {str(segmentIDs)}. Now you are supposed to write a summary of this. That summary should be indicative of what the document contains and one line description of the agency, transaction set and each segment.
    '''
    output = ask_mixtral({
        "inputs": question,
        "parameters": {
            "decoding_method": "greedy",
            "min_new_tokens": 1,
            "max_new_tokens": 600
        }
    })
    return output[0]["generated_text"][len(question):]


def get_versions(session, agency):
    query = "MATCH (a:Agency {agencyID: $agency})-->(v:Version) RETURN v LIMIT 50"
    result = session.run(query, agency=agency)
    details = []
    for record in result:
        # print(record)
        details.append(record['v']._properties["version"])
        # details.append({
        #   "label": record['v']._properties["version"],
        #   "value": {
        #     "input": {
        #       "text": record['v']._properties["version"]
        #     }
        #   }
        # })
    return details


def get_tss(session, agency, version):
    query = "MATCH (a:Agency {agencyID: $agency})-->(v:Version {version: $version})-->(ts: TransactionSet) RETURN ts LIMIT 50"
    result = session.run(query, agency=agency, version=version)
    details = []
    for record in result:
        # print(record)
        details.append(record['ts']._properties["TransactionSet"])
        # details.append({
        #   "label": record['ts']._properties["TransactionSet"],
        #   "value": {
        #     "input": {
        #       "text": record['ts']._properties["TransactionSet"]
        #     }
        #   }
        # })
    return details


def get_segments(session, agency, version, ts):
    query = "MATCH (a:Agency {agencyID: $agency})-->(v:Version {version: $version})-->(ts: TransactionSet {TransactionSet: $ts})-->(s: Segment) RETURN s LIMIT 50"
    result = session.run(query, agency=agency, version=version, ts=ts)
    details = []
    for record in result:
        # print(record)
        details.append(record['s']._properties["SegmentID"])
        # details.append({
        #   "label": record['s']._properties["SegmentID"],
        #   "value": {
        #     "input": {
        #       "text": record['s']._properties["SegmentID"]
        #     }
        #   }
        # })
    return details


def get_elements(session, agency, version, ts, segments):
    query = "MATCH (a:Agency {agencyID: $agency})-->(v:Version {version: $version})-->(ts: TransactionSet {TransactionSet: $ts})-->(s: Segment)-->(e: Element) WHERE s.SegmentID IN $segments RETURN e LIMIT 30"
    result = session.run(query, agency=agency, version=version, ts=ts, segments=segments)
    details = []
    for record in result:
        # print(record)
        details.append(record['e']._properties)
    return details



def get_info(session, agency, version, ts, segments, name, ftype='pdf'):
    query = "MATCH (a:Agency {agencyID: $agency})-->(v:Version {version: $version})-->(ts: TransactionSet {TransactionSet: $ts})-->(s:Segment) WHERE s.SegmentID IN $segments RETURN s"
    result = session.run(query, agency=agency, version=version, ts=ts, segments=segments)
    details = {
        "Agency": {},
        "Version": {},
        "TransactionSet": {},
        "Segments": [],
        "Elements": []
    }
    for record in result:
        details["Segments"].append(record['s']._properties)
    print(details["Segments"])

    details["Elements"] = get_elements(session, agency, version, ts, segments)

    query = "MATCH (a:Agency {agencyID: $agency})-->(v:Version {version: $version})-->(ts: TransactionSet {TransactionSet: $ts}) RETURN a,v,ts"
    result = session.run(query, agency=agency, version=version, ts=ts)

    for record in result:
        details["Agency"] = record['a']._properties
        details["Version"] = record['v']._properties
        details["TransactionSet"] = record['ts']._properties
    if ftype == 'pdf':
        seg_columns = ["Position", "SegmentID", "SegmentDescription", "Section", "RequirementDesignator", "MaximumUsage", "LoopID",
                       "Notes"]
        all_segments = pd.DataFrame(details["Segments"], columns=seg_columns)
        all_segments = all_segments.drop_duplicates(['SegmentID']).reset_index()
        all_segments.sort_values(by=["Position"], inplace=True, key=lambda x: pd.to_numeric(x, errors="coerce"))
        for iii, s in all_segments.iterrows():
            print(iii, s["Position"])
        all_segments.fillna("", inplace=True)
        all_segments.reset_index(inplace=True)
        all_segments.replace("Undefined", "N/A", inplace=True)
        element_columns = ["ElementID", "SegmentID", "RequirementDesignator", "Description", "Type", "MinimumLength",
                           "MaximumLength", "Ref", "Notes"]
        all_elements = pd.DataFrame(details["Elements"], columns=element_columns)
        all_elements = all_elements.drop_duplicates().reset_index()
        all_elements.fillna("", inplace=True)
        all_elements.replace("Undefined", "N/A", inplace=True)
        pdf_buffer = create_pdf(details["TransactionSet"]["TransactionSet"], details["Version"]["version"], all_segments, all_elements)
        # Now you can save the buffer contents into a file if needed
        # multi_part_upload("edi-bucket", name+"_edi.pdf", pdf_buffer)
        with open("output_try.pdf", "wb") as f:
            f.write(pdf_buffer.read())
        return "https://edi-bucket.s3.us-south.cloud-object-storage.appdomain.cloud/"+name+"_edi.pdf"
    # else:
    #     create_word_doc_with_dict({"results": details})
    return details


def main(args):
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    details = []
    with driver.session() as session:
        agency = args.get("agency")
        version = args.get("version")
        tset = args.get("tset")
        segments = args.get("segments")
        if segments:
            segments = segments.split(", ")
        ftype = args.get("ftype")
        name = args.get("name")
        title = "Please select "
        if segments:
            details = get_info(session, agency, version, tset, segments, name, ftype)
            return {"results": details}
        # elif segment:
        #     details = get_elements(session, agency, version, tset, segment)
        #     title += "an element"
        elif tset:
            details = get_segments(session, agency, version, tset)
            title += "a segment"
        elif version:
            details = get_tss(session, agency, version)
            title += "a transaction set"
        elif agency:
            details = get_versions(session, agency)
            title += "a version"
    driver.close()
    # return {"results": [
    #     {
    #         "title": title,
    #         "options": details,
    #         "description": "",
    #         "response_type": "option"
    #     }
    # ]}
    return {"results": details}


# print(main({"agency": "E", "version": "092001", "fg": "CONEST", "tset": "CONEST", "segment": "BII",
#             "element": "7429", "code": "2", "name": "kedar", "ftype": "docx"}))
# pdf_buffer = create_pdf_with_dict({"11":"heloo","new":{"2":"hello2","3":"hello3"}})
#
# # Now you can save the buffer contents into a file if needed
# with open("output.pdf", "wb") as f:
#     f.write(pdf_buffer.read())
# create_word_doc_with_dict({"11":"heloo","new":{"2":"hello2","3":"hello3"}})
# print(main({"agency": "E"}))
# main({"agency": "E", "version": "092001", "fg": "CONEST", "tset": "CONEST", "segment": "BII",
#             "element": "7429", "code": "2", "name": "kedar", "ftype": "pdf"})
# print(main({"agency": "X", "version": "004020", "tset": "850", "segment": "BEG", "element": "0353", "name": "kedar", "ftype": "pdf"}))


# print(main({"agency": "X", "version": "004010TI0900", "tset": "850", "segments": "BEG, PO1, ISA", "name": "kedar", "ftype": "pdf"}))


if __name__ == "__main__":

    users = {
        "user1": {"Password": "pswd1", "Location": "North America", "Industry": "manufacturing"},
        "user2": {"Password": "pswd2", "Location": "Germany", "Industry": "logistics"}
    }

    agencies = ['A', 'E', 'O', 'T', 'U', 'X']
    agencies_description = ["Tradacoms", "Edifact", "ODETTE", "TDCC", "UCS", "X12"]
    mandatory_segments_x12 = ["ISA", "GS", "ST", "BEG", "PO1", "SE", "GE", "IEA"]
    mandatory_segments_edifact = ["UNB", "UNH", "BGM", "LIN", "UNT", "UNZ"]

    username = input("Enter your username: ")
    if username not in users.keys():
        print("User does not exist")
        exit(1)
    password = input("Enter your password: ")
    if password != users[username]["Password"]:
        print("Wrong username or password")
        exit(2)
    for i in range(len(agencies)):
        print(str(i+1) + ". " + agencies[i] + " - " + agencies_description[i])
    agency = input("Choose the agency code from above: ")
    agency_description = agencies_description[agencies.index(agency)]
    versions = main({'agency': agency})["results"]
    print("Following versions are available:")
    print(versions)
    version = input("Choose the version from above: ")
    # tsets = main({'agency': agency, "version": version})["results"]
    # print("Following transaction sets / messages are available:")
    # print(tsets)
    question = f'''
    You are an expert in EDI messages and specifications. A user from {users[username]["Location"]} who has {users[username]["Industry"]} business enters your system. You are required to display the most used {agency_description} transaction sets for him based on his geography and industry. Your outputs should only have the top 3 Transaction Set IDs. For example if the IDs are X, Y, Z (these are just example not real IDs), then output should be:
    X, Y, Z

    You response should have no explanation or anything else. Only IDs.
    There should be nothing else in your response.
    '''
    output = ask_mixtral({
        "inputs": question,
        "parameters": {
            "decoding_method": "greedy",
            "min_new_tokens": 1,
            "max_new_tokens": 200
        }
    })
    print("\n\n")
    print(output[0]["generated_text"][len(question):])
    tset = input("Choose the transaction set from above or type 'other': ")
    if tset == "other":
        tsets = main({'agency': agency, "version": version})["results"]
        print("Following transaction sets / messages are available:")
        print(tsets)
        tset = input("Choose the transaction set from above: ")
    segments = main({'agency': agency, "version": version, "tset": tset})["results"]
    mandatory = mandatory_segments_x12 if agency == 'X' else mandatory_segments_edifact
    mandatory = [i for i in mandatory if i in segments]
    other_segments = set([i for i in segments if i not in mandatory])
    print("These segments will be added to the specification file.")
    print(mandatory)
    print("Other segments are:")
    print(other_segments)
    segments = input("If you want to add more segments, choose from above or type 'No':")
    if segments == "No":
        segments = ""
    segments = mandatory + segments.split(", ")
    segments = ", ".join(segments)
    details = main({'agency': agency, "version": version, "tset": tset, "segments": segments, "name": "kedar", "ftype": "pdf"})["results"]
    print("EDI spec file generated successfully.")

