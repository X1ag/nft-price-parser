from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

ANONYMOUS_TG_NUMBERS = 'EQAOQdwdw8kGftJCSFgOErM1mBjYPe4DBPq8-AhF6vr9si5N'
TG_USERNAMES = 'EQCA14o1-VWhS2efqoh_9M1b_A9DtKTuoqfmkn83AbJzwnPi'

async def get_nft_collection_floor(nft_collection_address: str):
  API_PATH = "https://api.getgems.io/graphql"

  QUERY = gql(
          """query AlphaNftCollectionStats($address: String!) {
          alphaNftCollectionStats(address: $address) {
          floorPrice
          }
          }"""
 )

  try:
    transport = AIOHTTPTransport(url=API_PATH)
    async with Client(transport=transport, fetch_schema_from_transport=True) as session:
      result = await session.execute(QUERY,variable_values={"address": nft_collection_address})
      return result["alphaNftCollectionStats"]["floorPrice"]
  except:
    return None


 
