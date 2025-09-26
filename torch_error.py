# confluence_ingest.py
import os
import requests
from bs4 import BeautifulSoup
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer
import json
from typing import List, Dict
from opensearch.OpenSearchApi import *
from opensearch.opensearchembedding import load_model_from_jfrog,JFROG_REPO_KEY,JFROG_ARTIFACT,JFROG_TOKEN,JFROG_BASE_URL,EXTRACT_DIR,MODEL_NAME

CONFLUENCE_BASE = os.getenv("CONFLUENCE_BASE")  # e.g. https://your-domain.atlassian.net/wiki
CONFLUENCE_USER = os.getenv("CONFLUENCE_USER")
CONFLUENCE_TOKEN = os.getenv("CONFLUENCE_TOKEN")
OPENSEARCH = "https://opensearch-gaas.se-int-caas.ca.cib/"
# my cluster url 
# https://opensearch-atlasrag-dashboard.se-int-caas.ca.cib/  
# https://opensearch-atlasrag.se-int-caas.ca.cib/  
# Connection config
host = "opensearch-atlasrag.se-int-caas.ca.cib"
port = 443
auth = ("admin", "admin")  # basic auth


INDEX = "confluence"
#EMBED_MODEL = "all-MiniLM-L6-v2"

model_dir = load_model_from_jfrog(JFROG_REPO_KEY, JFROG_ARTIFACT, JFROG_TOKEN, extract_to=EXTRACT_DIR)
model = SentenceTransformer(model_dir)
# proxies = {
#      "http":  "http://SRV_DBO1PRD:+Vf83$psl!yq62i0@proxy.par.emea.cib:8080",
#      "https": "http://svc_linux_web:oD10i9fr@proxy.par.emea.cib:8080"
# }



#os.environ["HTTP_PROXY"] = "http://svc_linux_web:oD10i9fr@proxy.par.emea.cib:8080"
#os.environ["HTTPS_PROXY"] = "http://svc_linux_web:oD10i9fr@proxy.par.emea.cib:8080"
#client = OpenSearch(OPENSEARCH)
# If HTTPS with self-signed certs, set verify_certs=False
# client = OpenSearch(
#      hosts=[{"host": host, "port": port}],
#      http_auth=auth,
#      use_ssl=True,              # if your cluster runs on https
#      verify_certs=False,        # True if you trust CA, False to skip SSL check
#      ssl_show_warn=False
#  )

# Test the connection
api = OpenSearchAPI(
    host="opensearch-atlasrag.se-int-caas.ca.cib",
    port=443,
    username="admin",
    password="admin",
    use_ssl=True,
    verify_certs=False
)

#api.create_index(index=INDEX, mapping_file="opensearch_mapping.json")



def fetch_pages(space="ENG", limit=50):
    url = f"{CONFLUENCE_BASE}/rest/api/content"
    params = {
        "spaceKey": space,
        "limit": limit,
        "expand": "body.storage"
    }
    resp = requests.get(url, params=params, auth=(CONFLUENCE_USER, CONFLUENCE_TOKEN))
    resp.raise_for_status()
    return resp.json().get("results", [])

def clean_html(content):
   
    soup = BeautifulSoup(content, "html.parser")
    return soup.get_text(" ", strip=True)
   
def ingest_space(pages,space):
    #pages = fetch_pages(space, limit=max_pages)
    for page in pages:
        page_id = page["id"]
        title = page["title"]
        # Get the ancestors list from the response
        url = page['url']
        parent = page['parent']
        attachments=page['attachments']
        comments = page['comments']
        text = clean_html(page['content'])
        chunks = [text[i:i+1500] for i in range(0, len(text), 1500)]
        for i, chunk in enumerate(chunks):
            emb = model.encode(chunk).tolist()
            doc = {
                "title": title,
                "url": url,
                "text": chunk,
                "parent" : parent, 
                "metadata": {"space": space, "page_id": page_id, "chunk": i},
                "embedding": emb,
            }
            api.index_doc(INDEX, i, doc, mapping_file="opensearch/opensearch_mapping.json")
    print(f"Ingested {len(pages)} pages from space {space}")


def ingest_page(page):
    #pages = fetch_pages(space, limit=max_pages)
    page_id = page["id"]
    title = page["title"]
    # Get the ancestors list from the response
    url = page['url']
    parent = page['parent']
    attachments=page['attachments']
    comments = page['comments']
    text = clean_html(page['content'])
    chunks = [text[i:i+1500] for i in range(0, len(text), 1500)]
    for i, chunk in enumerate(chunks):
        emb = model.encode(chunk).tolist()
        doc = {
            "title": title,
            "url": url,
            "text": chunk,
            "parent" : parent, 
            "metadata": {"space": {page['space']}, "page_id": page_id, "chunk": i},
            "embedding": emb,
        }
        api.index_doc(INDEX, i, doc, mapping_file="opensearch/opensearch_mapping.json")
    print(f"Ingested {page['title']} pages from space {page['space']}")




def get_page_with_children(page_id: str, base_url: str, api_token: str, email: str) -> dict:
    """
    Récupère une page Confluence et ses pages enfants en utilisant l'API REST v2.

    Args:
        page_id: L'identifiant unique de la page Confluence.
        base_url: L'URL de base de votre instance Confluence (ex: 'https://votresite.atlassian.net').
        api_token: Le jeton d'API généré dans votre compte Atlassian.
        email: Votre adresse email de connexion à Atlassian.

    Returns:
        Un dictionnaire contenant les données de la page parent et la liste des enfants,
        ou un dictionnaire vide si une erreur survient.
    """
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    # URL de l'API pour récupérer une page
    page_url = f"{base_url}/wiki/api/v2/pages/{page_id}"
    
    # URL de l'API pour récupérer les enfants d'une page
    children_url = f"{base_url}/wiki/api/v2/pages/{page_id}/children"

    results = {
        'parent_page': None,
        'children': []
    }

    try:
        # 1. Récupération de la page parent
        response_parent = requests.get(page_url, headers=headers, auth=(email, api_token))
        response_parent.raise_for_status() # Lève une exception si le statut est une erreur (4xx ou 5xx)
        results['parent_page'] = response_parent.json()

        # 2. Récupération des pages enfants
        response_children = requests.get(children_url, headers=headers, auth=(email, api_token))
        response_children.raise_for_status()
        
        # L'API des enfants renvoie un objet JSON avec une liste dans la clé 'results'
        results['children'] = response_children.json().get('results', [])

    except requests.exceptions.HTTPError as err:
        print(f"Erreur HTTP: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Une erreur s'est produite: {err}")
    
    return results



# def _fetch_paginated_data(url: str, auth: tuple) -> list:
#     """
#     Fonction utilitaire pour gérer la pagination et récupérer toutes les données d'un endpoint.
#     """
#     all_data = []
#     while url:
#         try:
#             response = requests.get(url, auth=auth)
#             response.raise_for_status()
#             data = response.json()
            
#             # Les données se trouvent généralement dans la clé 'results'
#             all_data.extend(data.get('results', []))

#             # Vérifier s'il y a un lien pour la page suivante
#             next_link = data.get('_links', {}).get('next')
#             if next_link:
#                 url = f"{response.url.split('?')[0]}{next_link}"
#             else:
#                 url = None
#         except requests.exceptions.HTTPError as err:
#             print(f"Erreur HTTP: {err}")
#             return []
#         except requests.exceptions.RequestException as err:
#             print(f"Une erreur de connexion s'est produite: {err}")
#             return []
            
#     return all_data


def _fetch_paginated_data(url: str, auth: tuple,cert_file) -> List[Dict]:
    """
    Fonction utilitaire pour gérer la pagination et récupérer toutes les données d'un endpoint.
    """
    all_data = []
    cursor = None
    email,token = auth
    headers ={
        "Authorization" : f"Bearer: {token}",
        "Accept": "application/json"
    }
    while True:
        current_url = url
        if cursor:
            current_url += f"&cursor={cursor}" if '?' in current_url else f"?cursor={cursor}"
        
        try:
            #response = requests.get(current_url, auth=auth,verify=False)
            response = requests.get(current_url, headers=headers,verify=False)
            response.raise_for_status()
            data = response.json()
            
            # Les données se trouvent dans la clé 'results' pour l'API v2
            all_data.extend(data.get('results', []))

            # L'API v2 utilise un 'cursor' pour la pagination
            cursor = data.get('cursor')
            if not cursor:
                break
        except requests.exceptions.RequestException as err:
            print(f"Erreur lors de l'appel à {current_url}: {err}")
            return []
            
    return all_data




def get_all_confluence_pages(base_url: str, api_token: str, email: str,space: str) -> list:
    """
    Récupère toutes les pages de tous les espaces d'une instance Confluence.

    Args:
        base_url: L'URL de base de votre instance Confluence.
        api_token: Le jeton d'API de votre compte Atlassian.
        email: Votre adresse email de connexion.

    Returns:
        Une liste de tous les objets de page trouvés.
    """
    auth = (email, api_token)
    all_pages = []

    # 1. Récupérer tous les espaces
    #spaces_url = f"{base_url}/wiki/api/v2/spaces"
    spaces_url = f"{base_url}/rest/api/space"
    print("Récupération des espaces en cours...")
    all_spaces = _fetch_paginated_data(spaces_url, auth)
    print(f"{len(all_spaces)} espaces trouvés.")

    # 2. Pour chaque espace, récupérer toutes ses pages
    for space in all_spaces:
        if space.get('name') == MySpace:
            space_id = space.get('id')
            space_key = space.get('key')
            print(f"  -> Récupération des pages pour l'espace '{space_key}' ({space_id})...")            
            pages_url = f"{base_url}/wiki/api/v2/spaces/{space_id}/pages"
            space_pages = _fetch_paginated_data(pages_url, auth)
            all_pages.extend(space_pages)

    return all_pages



def get_all_confluence_pages_full(base_url: str, api_token: str, email: str,space: str,cert_file) -> List[Dict]:
    """
    Récupère toutes les pages, ainsi que leurs commentaires et pièces jointes, de tous les espaces.

    Args:
        base_url: L'URL de base de votre instance Confluence.
        api_token: Le jeton d'API de votre compte Atlassian.
        email: Votre adresse email de connexion.

    Returns:
        Une liste de tous les objets de page enrichis.
    """
    auth = (email, api_token)
    all_pages = []

    # 1. Récupérer tous les espaces
    spaces_url = f"{base_url}/rest/api/space"
    print("Récupération des espaces en cours...")
    all_spaces = _fetch_paginated_data(spaces_url, auth,cert_file)
    print(f"{len(all_spaces)} espaces trouvés.")

    # 2. Pour chaque espace, récupérer toutes ses pages, commentaires et pièces jointes
    for space in all_spaces:
        space_key = space.get('key')
        space_id = space.get('id')
        print(f"space name is {space.get('name')} space key is {space_key}")
        #if space.get('name') == MySpace:

        print(f"\n-> Récupération des pages et de leur contenu pour l'espace '{space_key}'...")
        
        pages_url = f"{base_url}/rest/api/content?spaceKey={space_key}&type=page&limit=2&expand=body.storage,version,children.attachments,children.comment"
        space_pages = _fetch_paginated_data(pages_url, auth,cert_file)

        for page in space_pages:
            page_id = page.get('id')
            page['space'] = space_key
            #content_url = f"{base_url}/rest/api/content/{page_id}?expand=body.storage,version,children.attachments,children.comment"
            #content_response = requests.get(content_url, auth=auth,verify=False)
            #content_response.raise_for_status()
            #content_data = content_response.json()
            url = f"{base_url}/{page['_links']['webui']}"
            page['url'] = url
            page_content = page.get('body', {}).get('storage', {}).get('value', 'Contenu non disponible')
            page['content'] = page_content
            # Récupération des pièces jointes de la page
            attachments = page.get("children",{}).get('attachments', {}).get('results', [])
            page['attachments'] = attachments
            # Récupération des commentaires de la page
            comments = page.get("children",{}).get('comments', {}).get('results', [])
            page['comments'] = comments
                # Get the ancestors list from the response
            ancestors = page.get('ancestors', [])
    
                # The parent is the last item in the ancestors list
            if ancestors:
                parent_page = ancestors[-1]
                # Store the parent data as metadata in a new key
                page['parent'] = {'parent': parent_page}
        
            else:
                # If there are no ancestors, the page is a top-level page
                page['parent'] = {'parent': None}


            all_pages.append(page)
            print(f"   -> Page '{page.get('title')}': {len(attachments)} attachements, {len(comments)} commentaires.")

    return all_pages


# --- Exemple d'utilisation ---
if __name__ == "__main__":
    # Remplacer cet url  https://wiki.saas.cagip.group.gca/ par le votre
    CONFLUENCE_BASE_URL = "https://wiki.saas.cagip.group.gca"
    YOUR_EMAIL = "jean-folly.kpodar-prestataire@ca-gip.fr"
    YOUR_API_TOKEN = "mytoken"
    CONFLUENCE_USER="u50ho63@zoe.gca"
    PAGE_ID_TO_FETCH = "123456789" # Remplacer par l'ID de la page que vous voulez récupérer
    MySpace="CCPDATAIA"
    #MySpace="CCGP"
    cafile="opensearch-wiki-confluence-qa-full/cabundle.pem"

    all_confluence_pages = get_all_confluence_pages_full(
        base_url=CONFLUENCE_BASE_URL,
        api_token=YOUR_API_TOKEN,
        email=CONFLUENCE_USER,
        space=MySpace,
        cert_file=cafile
    )

    if all_confluence_pages:

        print("\n--- Récupération terminée ---")
        print(f"Nombre total de pages trouvées : {len(all_confluence_pages)}")
        print("\nExemple des 5 premières pages :")
        for page in all_confluence_pages[:5]:
            print(f" ->  Page '{page.get('title')}': ID: '{page.get('id')}', '{len(page.get('attachments'))}' \
                  attachements, '{len(page.get('comments'))}' commentaires  '{page['parent']}' parent  \
                 url: {page.get('url')} content: {clean_html(page.get("content"))} . ")
            if len(page.get('attachments')) > 0 :
                for attach in page.get('attachements') :
                    print(f"attachement ,{attach.get('title')} ,{attach.get('_links',{})}" ,  {attach.get('download')})
            if len(page.get('comments')) > 0 :
                for c in page.get('comments'):
                    print(f"comment by {c.get('creator', {}).get('DisplayName')} \
                        comment: {c.get('body',{}).get('storage',{}).get('value')}")
            # page_data = get_page_with_children(
            # page_id=page.get('id'),
            # base_url=CONFLUENCE_BASE_URL,
            # api_token=YOUR_API_TOKEN,
            # email=YOUR_EMAIL
            # )

            # if page_data.get('parent_page'):
            #     parent_title = page_data['parent_page'].get('title')
            #     print(f"Page parente: '{parent_title}'")
            #     print(f"Nombre de pages enfants trouvées: {len(page_data['children'])}")
            #     ingest_space()
            #     if page_data['children']:
            #         print("\nPages enfants:")
            #         for child in page_data['children']:
            #             print(f"- Titre: {child.get('title')}, ID: {child.get('id')}")
            # else:
            #     print("Échec de la récupération des données de la page.")
            ingest_page(page)
    else:
        print("Aucune page n'a pu être récupérée. Vérifiez vos informations d'identification.")


opensearchpy.exceptions.SerializationError: ({'title': 'BWAY Internal trades', 'url': 'https://wiki.saas.cagip.group.gca//spaces/BS/pages/83776852/BWAY+Internal+trades', 'text': 'Credit Traders trading USTreasury with UST Traders via Bloomberg – Broadway and JetStream TradeCapture Configuration 1 Background Global CA-CIB Credit Traders can raise an RFQ on Bloomberg/Broadway trading USTreasury bond with CA-CIB UST Traders globally. 2 scenario : where Credit Trader trading USTreasury with UST Murex trader. One trade is sent to Murex and Murex creates 2 sides of the trade, one for Credit Trader and one for UST Trader where Credit Trader trading USTreasury with UST OT trader.\xa0 One trade is sent to OT,the other side of the trade for Credit trader is sent to Murex => 2 files are generated Workflow Diagrams below illustrates the UST internal trading via Bloomberg - Trading Scenario Murex Credit Trader trading with OT UST Trader Two Trade CIBML are created.\xa0 One for UST OT Trader, another for Creit Murex trader The trade leg for the Credit trader needs to be sent to Murex Cross Entity (NY Entity faces Paris Entity) Credit NY Desk(Murex users) trade USTreasury with UST LDN/TKY Desk(OT users) Same Entity (Paris Entity faces Paris Entity) Credit LDN/NY Desk(Murex users) trade USTreasury with LDN/TKY Desk(OT users) Operational Configuration Use Broadway MO tool => New Tab => Counterparties Configure UST traders Counterparty Fields Value Description Account Type Account Indication of\xa0 BloombergUST\xa0 Internal Credit<->UST trading Full Name OT or MUREX Risk System, OT or MUREX Account Id NYEntity or PARISEntity The Entity where the trader belongs to Short Name Broadw', 'parent': {'parent': None}, 'metadata': {'space': {'BS'}, 'page_id': '83776852', 'chunk': 0}, 'embedding': [-0.08941161632537842, -0.09077267348766327, -0.005863995291292667, -0.09424741566181183, -0.05120771750807762, 0.02999376505613327, 0.019798535853624344, 0.07541586458683014, 0.04850892350077629, -0.028913985937833786, -0.015708228573203087, -0.0999000146985054, 0.009454070590436459, 0.03880094364285469, -0.048874080181121826, -0.01961447298526764, 0.022645050659775734, -0.02857259102165699, 0.03281460329890251, 0.05335697531700134, 0.09248396009206772, -0.0480642206966877, -0.004075510427355766, -0.043749913573265076, 0.013641029596328735, -0.02474178560078144, -0.03395919129252434, 0.04024452343583107, 0.02734323777258396, -0.033210087567567825, 0.018360333517193794, 0.08544903993606567, 0.07119674980640411, -0.00272704497911036, -0.019043121486902237, 0.043236877769231796, -0.0033657336607575417, -0.020334739238023758, 0.028981653973460197, -0.04907615855336189, 0.006616006605327129, 0.03662586957216263, -0.017543325200676918, 0.01997583545744419, -0.042589399963617325, -0.04392005503177643, -0.00030933404923416674, 0.04049542173743248, -0.08317793905735016, 0.09295583516359329, -0.07760652154684067, -0.006418159231543541, -0.022966599091887474, 0.07786092907190323, -0.008001205511391163, 0.06770821660757065, -0.02181156910955906, 0.018451688811182976, 0.03796694427728653, -0.05837105214595795, -0.02781929075717926, -0.00014738456229679286, 0.054685067385435104, -0.003969825804233551, 0.0054815891198813915, -0.0005881238612346351, -0.05005890876054764, 0.05364939942955971, -0.053032878786325455, -0.08973029255867004, -0.04355669021606445, -0.05275847017765045, -0.1412682682275772, -0.023596512153744698, 0.04626213386654854, 0.048845626413822174, 0.01748449169099331, -0.032548997551202774, -0.041008707135915756, -0.0790627971291542, 0.011816499754786491, 0.04297797009348869, 0.022665711119771004, -0.12336084991693497, 0.06333106011152267, -0.0013019859325140715, -0.013492021709680557, -0.004588950425386429, 0.07843983918428421, -0.010599307715892792, 0.10800434648990631, 0.008222165517508984, 0.01922312006354332, -0.037231866270303726, 0.00964395422488451, 0.00608091801404953, -0.03977999836206436, 0.12460939586162567, 0.014271729625761509, 0.004784862976521254, 0.09128658473491669, 0.0693802684545517, -0.031111527234315872, 0.02023075334727764, -0.06842681020498276, -0.049919892102479935, 0.060722894966602325, -0.05190008133649826, -0.03972824662923813, -0.10150143504142761, -0.03478068485856056, 0.09200458228588104, -0.008661807514727116, -0.0615374818444252, -0.09043236076831818, 0.09769155085086823, -0.04952108487486839, -0.020862659439444542, 0.09931838512420654, -0.03595000505447388, -0.06094755604863167, 0.057271890342235565, -0.11431198567152023, 0.025753909721970558, -0.1118641272187233, 0.018885526806116104, -0.0264331866055727, 1.6309497756871586e-32, -0.041967205703258514, -0.014657998457551003, 0.012478763237595558, 0.009756986051797867, -0.09453129768371582, 0.03239128366112709, -0.004224178846925497, 0.01959139108657837, -0.11051332950592041, -0.04629949480295181, -0.02486584335565567, 0.02152855321764946, 0.009467030875384808, 0.013688305392861366, -0.09985852986574173, -0.03224175423383713, -0.013623511418700218, -0.0850769653916359, 0.008318244479596615, 0.0792895182967186, 0.06713717430830002, -0.01703854463994503, 0.02107056975364685, 0.03527173027396202, 0.06799663603305817, 0.022184064611792564, -0.026630602777004242, 0.0004807928344234824, 0.02766825631260872, 0.0077299559488892555, -0.07263767719268799, -0.025791851803660393, -0.006114629562944174, 0.058292049914598465, -0.021496104076504707, 0.07407377660274506, 0.004975476302206516, 0.017029128968715668, 0.01736193522810936, -0.013883792795240879, -0.036015287041664124, 0.06072276085615158, -0.039976272732019424, -0.05265895649790764, 0.051338132470846176, -0.05210232362151146, 0.03396626561880112, 0.029383106157183647, 0.007037023082375526, 0.06342712789773941, 0.02952071838080883, -0.006831783801317215, -0.04320358857512474, -0.06898760795593262, -0.07111328840255737, 0.03915294259786606, 0.007740352302789688, -0.03281887248158455, -0.03185581415891647, 0.034249141812324524, 0.006297977641224861, -0.06280853599309921, -0.01197401899844408, -0.0028304187580943108, -0.03464910015463829, 0.08673553913831711, -0.07210218161344528, -0.04371590167284012, -0.0349128432571888, 0.031425245106220245, -0.07695695757865906, 0.04577133432030678, 0.046806395053863525, 0.07187400758266449, 0.0536322258412838, -0.009533350355923176, -0.009511927142739296, 0.06996770948171616, 0.02971828728914261, -0.03386280685663223, -0.13393017649650574, 0.025313537567853928, 0.038109198212623596, 0.09400158375501633, -0.020447885617613792, 0.009202156215906143, 0.02186102420091629, -0.03589161857962608, -0.030752288177609444, 0.05612018704414368, -0.023226581513881683, -0.06490923464298248, 0.017259927466511726, 0.06040024384856224, 0.08646439015865326, -1.6320914745753948e-32, -0.054079215973615646, 0.003353749867528677, -0.046884551644325256, -0.06335607916116714, -0.0649016946554184, -0.027829105034470558, 0.015824295580387115, -0.04488326236605644, 0.07953944057226181, -0.032502710819244385, -0.019832922145724297, -0.029535168781876564, 0.0012580789625644684, -0.010791843757033348, 0.024909084662795067, -0.008927619084715843, 0.042701028287410736, 0.04403175413608551, -0.06322295218706131, -0.03341101482510567, 0.1310417354106903, 0.06180999428033829, -0.0241586621850729, 0.08043087273836136, 0.03297019004821777, 0.06576171517372131, 0.029369689524173737, 0.008238994516432285, 0.028933266177773476, 0.03481348976492882, -0.06864986568689346, -0.005614131223410368, 0.0007044169469736516, 0.05463588237762451, -0.0625358298420906, -0.029790939763188362, 0.0187984686344862, 0.03625965490937233, 0.009233774617314339, 0.005410546436905861, 0.007578910328447819, -0.0018590984400361776, -0.023688042536377907, 0.10114497691392899, -0.04246138036251068, 0.06299395114183426, -0.0529024600982666, -0.04248295724391937, -0.002145239617675543, 0.01616520620882511, -0.08054795861244202, 0.05888254940509796, -0.06313890963792801, -0.0436524823307991, -0.06156649440526962, 0.04467833787202835, 0.05829079821705818, -0.06957218796014786, -0.017450256273150444, -0.10255314409732819, -0.023266872391104698, 0.11395163089036942, 0.0356733575463295, -0.03232954815030098, 0.0765441432595253, -0.04487810656428337, 0.022270066663622856, -0.039968714118003845, -0.008840460330247879, 0.002068202942609787, 0.07278378307819366, 0.01485340017825365, -0.04777541756629944, -0.12616774439811707, 0.09468086063861847, 0.05043454468250275, -0.07196089625358582, -0.0070993308909237385, 0.0012528630904853344, 0.02355950139462948, -0.04072605073451996, 0.016656769439578056, 0.0875982716679573, 0.0746271014213562, 0.021021874621510506, 0.014273291453719139, 0.03320251777768135, -0.010898764245212078, 0.031952425837516785, -0.039218321442604065, -0.05501020327210426, -0.059320490807294846, 0.03998332470655441, -0.05011237412691116, -0.007506579160690308, -6.319536538512693e-08, -0.06645781546831131, -0.027679627761244774, -0.11101981997489929, 0.025525720790028572, -0.05682175233960152, -0.0005723166977986693, -0.07435502856969833, 0.026669925078749657, -0.03706781193614006, 0.02721848152577877, 0.04056458920240402, 0.005767638795077801, -0.06621229648590088, -0.1130194291472435, 0.022437093779444695, -0.006395595148205757, 0.0007083176169544458, -0.05016883462667465, -0.059307001531124115, -0.021797336637973785, -0.02636685222387314, 0.03928859159350395, -0.005214073695242405, 0.08369536697864532, 0.010543974116444588, -0.01506035402417183, 0.028825189918279648, 0.06690037250518799, 0.08269897848367691, -0.028027057647705078, -0.02813568525016308, 0.002105887047946453, 0.03185821697115898, -0.013700014911592007, -0.000815192237496376, -0.044205132871866226, 0.07062283158302307, 0.03345112502574921, -0.037036001682281494, 0.04551640525460243, 0.025165287777781487, -0.05705615133047104, -0.05308673158288002, -0.02158832550048828, 0.057845719158649445, 0.03293817490339279, -0.09065460413694382, -0.019472582265734673, 0.11941784620285034, -0.027529437094926834, -0.022186895832419395, -0.050140026956796646, 0.02964942529797554, 0.07016275823116302, -0.013111168518662453, 0.004401649348437786, -0.044611748307943344, 0.012715023010969162, 0.0923394113779068, -0.09822232276201248, 0.028805436566472054, 0.005901455879211426, -0.003129029180854559, 0.0026741407345980406]}, TypeError("Unable to serialize {'BS'} (type: <class 'set'>)")) 
