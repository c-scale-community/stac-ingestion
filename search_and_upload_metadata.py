#################################################
#                                               #
#               LIP / INCD 2022                 #
#                                               #
#   Authors:                                    #
#           Zacarias Benta - zacarias@lip.pt    # 
#           César Ferreira - cesar@lip.pt       #
#                                               #
#################################################
from email.mime import base
import json
from xml.dom.expatbuilder import parseString
import xml.etree.ElementTree as ET
from xml.dom import minidom
import traceback
import urllib3
import xmltodict
import requests
from pystac_client import Client
from re import I
from bs4 import BeautifulSoup
import urllib.request
api = Client.open('https://earth-search.aws.element84.com/v0')

#define the spatial data to collect
geom =  {
  "type": "Polygon",
  "coordinates": [
    [
      [
        -7.682155041704681,
        38.620982842287496
      ],
      [
        -7.682155041704681,
        36.18203953636458
      ],
      [
        -5.083888440142181,
        36.18203953636458
      ],
      [
        -5.083888440142181,
        38.620982842287496
      ],
      [
        -7.682155041704681,
        38.620982842287496
      ]
    ]
  ]
}

# concatenate the spatial and temporal data and collections data to be queried
search = api.search(
    collections = "sentinel-s2-l1c",
    intersects = geom,
    datetime = "2018-01-01/2021-01-01"
)

#get the nr of products matched
values=search.matched()

#create an products dictionary
itemdict=search.get_all_items_as_dict()

#list of existing files within INCD's S3 bucket
#existingfiles = ["S2B_MSIL1C_20201227T111359_N0209_R137_T29SPA_20201227T122128",  "S2B_MSIL1C_20201227T111359_N0209_R137_T30STH_20201227T122128", "S2A_MSIL1C_20201229T110451_N0209_R094_T29SQA_20201229T131620", "S2B_MSIL1C_20201227T111359_N0209_R137_T29SPB_20201227T122128", "S2B_MSIL1C_20201227T111359_N0209_R137_T30SUG_20201227T122128", "S2A_MSIL1C_20201229T110451_N0209_R094_T30STF_20201229T131620",  "S2B_MSIL1C_20201227T111359_N0209_R137_T29SPC_20201227T122128", "S2B_MSIL1C_20201227T111359_N0209_R137_T30SUH_20201227T122128", "S2A_MSIL1C_20201229T110451_N0209_R094_T30STG_20201229T131620", "S2B_MSIL1C_20201227T111359_N0209_R137_T29SQB_20201227T122128", "S2B_MSIL1C_20201230T112359_N0209_R037_T29SPA_20201230T122650", "S2A_MSIL1C_20201229T110451_N0209_R094_T30SUF_20201229T131620", "S2B_MSIL1C_20201227T111359_N0209_R137_T29SQC_20201227T122128", "S2B_MSIL1C_20201230T112359_N0209_R037_T29SPB_20201230T122650", "S2A_MSIL1C_20201229T110451_N0209_R094_T30SUG_20201229T131620",  "S2B_MSIL1C_20201227T111359_N0209_R137_T30STF_20201227T122128",  "S2B_MSIL1C_20201230T112359_N0209_R037_T29SPC_20201230T122650","S2A_MSIL1C_20201229T110451_N0209_R094_T30SUH_20201229T131620",  "S2B_MSIL1C_20201227T111359_N0209_R137_T30STG_20201227T122128",  "S2B_MSIL1C_20201230T112359_N0209_R037_T29SQC_20201230T122650"]
#file used for testing purposes
existingfiles = ["S2B_MSIL1C_20201227T111359_N0209_R137_T30STH_20201227T122128"]

#function to get metadata from within a XML file passsed as an url
# args:
#   url - url where the xml file is stored
#   object - the xml object you wish to locate within the xml file
#   tag - the tag within the xml object you wish to locate
#   id - the identification of the value within the tag you wish to locate
# returns:
#   metadata within the xml file   
def getMTD(url,object, tag, id):
	result=[]
	req = urllib.request.urlopen(url)
	xml = BeautifulSoup(req,'xml')
	if tag!="":
		for item in xml.find_all(object,{tag:id}):
			#print(item)
			for data in item:
				#print("DATA:", data)
				for meta in data:
					#print("META:",meta)
					strmeta=str(meta)
					if 'href' in strmeta : 
						#print("STRMETA",strmeta)
						result.append(strmeta)
	elif tag=="": 
		for item in xml.find_all(object):
			#print(item)
			for data in item:
				stritem=str(item)
				if id in  stritem:#print("DATA:", data)
					for meta in data:
						#print("META:",meta)
						strmeta=str(meta)
						if 'href' in strmeta : 
							#print("STRMETA",strmeta)
							result.append(strmeta)				
	return result


# function to retrieve the products metadata from https://earth-search.aws.element84.com/v0
# args:
#   itendict - item dictionary with all the products available for the specified spatial and temporal coordenates
#   filename - the product name we wisj to locate within the dictionary
# returns:
#   the json data regarding the specified product
def getmetadata(itemdict,filename):
  #indexes to stay clear of unwanted data within the dictionaries 
  idx1=0
  idx2=0
  for list in itemdict.items():
      #print(list)
      idx1+=1
      if idx1 > 1:
        for data in list:
          idx2+=1
          if idx2 > 1:
            for item in data:
              #get the json from the first list o jsons as a string
              jsonitem=item
              #convert into json format
              jsonitem=json.dumps(item, indent = 4)
              jsonitem=json.loads(jsonitem)
              #get the product identification from the json
              productID=jsonitem['properties']["sentinel:product_id"]
              #print(productID)
              #compare the filename wiht the productid
              if productID == filename:
                return jsonitem

#function used to replace the metadata that we get from https://earth-search.aws.element84.com/v0 
# and add the one we heve in our files
# args:
#   metadata - metadata in json format, extracted from https://earth-search.aws.element84.com/v0
#   identifier - the product identifier, for which we wish to replace the metadata
# returns:
#   a json file with all the metadata correctly pointing to INCD's S3 bucket      
def replacementadata(metadata,identifier):
  jsonitem=metadata
  #print("METADATA:")
  #print(jsonitem)
  identifiersafe=identifier[1:]
  identifiernosafe=identifier[1:-5] 
  baseurl="https://stratus-stor.ncg.ingrid.pt:8080/swift/v1/AUTH_8258f24c93b14473ba58892f5f2748f4/openeodata/out/"
  dataurl=baseurl+identifiersafe+"/"
  manifesturl=dataurl+"manifest.safe"
  #change collection name
  jsonitem['collection']="S2"
  #thumbnail
  thumbnail=dataurl+identifiernosafe+"-ql.jpg"
  jsonitem['assets']["thumbnail"]["href"]=thumbnail
  #print the resulting transformation
  #data=jsonitem['assets']["thumbnail"]["href"]
  #print("the data:",data)
  
  #overview tciimage
  tciimage=getMTD(manifesturl,"dataObject","ID","IMG_DATA_Band_TCI_Tile1_Data")
  tciimagepath=dataurl+tciimage[0][22:-21]
  jsonitem['assets']["overview"]["href"]=tciimagepath
  #print the resulting transformation
  #data=jsonitem['assets']["assets"]["href"]
  #print("the data:",data)
  
  #metadata
  meta_data=getMTD(manifesturl,"dataObject","ID","S2_Level-1C_Tile1_Metadata")
  metadatapath=dataurl+meta_data[0][22:-21]
  jsonitem['assets']["metadata"]["href"]=metadatapath
  #print the resulting transformation 
  #data=jsonitem['assets']["metadata"]["href"]
  #print("the data:",data)
  
  #info and links needs to be popped because we have no such information in our files
  jsonitem['assets'].pop('info')
  jsonitem.pop('links')  

  #images
  imagelist=getMTD(manifesturl,"dataObject","","IMG_DATA_Band_")
  #loop through existing images
  k=0
  for i in range(1,13):
    imagename=""
    if i<10:
      imagename="B0"+str(i)
    else:
      imagename="B"+str(i)
    #print the images
    #print("IMAGENAME:",imagename)
    #print("URL:",dataurl+imagelist[k][22:-21])  
    jsonitem['assets'][imagename]["href"]=dataurl+imagelist[k][22:-21]
    k+=1
  #print the resulting transformation 
  #data=jsonitem['assets']["B01"]["href"]
  #print("the image data:",data)

  overview=jsonitem['assets']["overview"]
  #info=jsonitem['assets']["info"]
  meta=jsonitem['assets']["metadata"]
  
  
  return jsonitem

cesneturl="https://resto.c-scale.zcu.cz/collections/S2/items"

# function used to register metadata at cesnet
# args:
#   cesneturl - the url to the stact catalog
#   json_data - the metadata we wish to register
# returns:
#   status code of the post request
# NOTE: please add the correct username and password to authenticate at cesnet    
def registercesnet(cesneturl,json_data):
  result=""
  header = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
  print("Sending Data to CESNET.")
  if json_data !="":
    send_data=json.dumps(json_data)
    print(send_data)
    result=requests.post(url=cesneturl,json=json_data,auth = ('username', 'PWD'),headers=header)
    print("Status Code:",result.status_code)
    print("Result:",result.text)
  return result.status_code


# loop through the porduct names existing in the list and register them at cesnet
for filename in existingfiles:
   metadata=""
   metadata=getmetadata(itemdict,filename)
   if metadata != "":
    print("PRODUCT",filename)
    #debug code
    # get all keys and values inside the json data
    #for item in metadata:
    #  print("Key : {} , Value : {}".format(item,metadata[item]))
    #print(metadata)

    thefile=filename+".SAFE"
    metadatatoregister=replacementadata(metadata,thefile)         
    registercesnet(cesneturl,metadatatoregister)