import scrapy
from neo4j import GraphDatabase

class PokeSpider(scrapy.Spider):
    name = 'pokespider'
    start_urls = ['https://pokemondb.net/pokedex/all']

    # Configurações para o Neo4j
    NEO4J_URI = "neo4j+s://d520607f.databases.neo4j.io"
    NEO4J_USERNAME = "neo4j"
    NEO4J_PASSWORD = "1GpRudGkDhHCk5AVhyNWbJKA9WiOZSR20gXWuEG8zsM"
    
    
    def __init__(self, *args, **kwargs):
        super(PokeSpider, self).__init__(*args, **kwargs)
        self.pokemon_data = []
        
        self.driver = GraphDatabase.driver(self.NEO4J_URI, auth=(self.NEO4J_USERNAME, self.NEO4J_PASSWORD))
        self.session = self.driver.session()


    def parse(self, response):
        pokemons_ls = response.css('table#pokedex > tbody > tr')
        for pokemon_l in pokemons_ls:
            href = pokemon_l.css('td:nth-child(2) > a::attr(href)').get()
            yield response.follow(href, self.parse_pokemon)

    def parse_pokemon(self, response):
        link = response.css(
            '#main > div.infocard-list-evo > div:nth-child(1) > span.infocard-lg-data.text-muted > a::attr(href)').get()
        pokemon_id = response.css('table.vitals-table > tbody > tr:nth-child(1) > td > strong::text').get()
        pokemon_name = response.css('#main > h1::text').get()
        pokemon_weight = response.css('table.vitals-table > tbody > tr:nth-child(5) > td::text').get()
        pokemon_height = response.css('table.vitals-table > tbody > tr:nth-child(4) > td::text').get()
        pokemon_type1 = response.css('table.vitals-table > tbody > tr:nth-child(2) > td > a:nth-child(1)::text').get()
        pokemon_type2 = response.css('table.vitals-table > tbody > tr:nth-child(2) > td > a:nth-child(2)::text').get()

        
        link_evolution1 = response.css(
            '#main > div.infocard-list-evo > div:nth-child(3) > span.infocard-lg-data.text-muted > a::attr(href)').get()
        evolution1 = response.css(
            '#main > div.infocard-list-evo > div:nth-child(3) > span.infocard-lg-data.text-muted > a::text').get()
        
        link_skill1 = response.css(
            'table.vitals-table > tbody > tr:nth-child(6) > td > span:nth-child(1) > a::attr(href)').get()
        skill1 = response.css(
            'table.vitals-table > tbody > tr:nth-child(6) > td > span:nth-child(1) > a::text').get()
        
        # Inserção no banco de dados Neo4j
        self.insert_pokemon(pokemon_id, pokemon_name, pokemon_weight, pokemon_height, pokemon_type1, pokemon_type2)
        

        if evolution1:
            self.insert_evolution(pokemon_id, evolution1, link_evolution1)

        if skill1:
            self.insert_skill(pokemon_id, skill1, link_skill1)
        
    def insert_pokemon(self, pokemon_id, pokemon_name, pokemon_weight, pokemon_height, pokemon_type1, pokemon_type2):
        query = """
        MERGE (p:Pokemon {id: $pokemon_id})
        SET p.name = $pokemon_name, p.weight = $pokemon_weight, p.height = $pokemon_height
        WITH p
        MERGE (t1:Type {name: $pokemon_type1})
        MERGE (p)-[:HAS_TYPE]->(t1)
        """
        
        if pokemon_type2:
            query += """
            MERGE (t2:Type {name: $pokemon_type2})
            MERGE (p)-[:HAS_TYPE]->(t2)
            """
        
        self.session.run(query, pokemon_id=pokemon_id, pokemon_name=pokemon_name, 
                         pokemon_weight=pokemon_weight, pokemon_height=pokemon_height, 
                         pokemon_type1=pokemon_type1, pokemon_type2=pokemon_type2)

    def insert_evolution(self, pokemon_id, evolution_name, evolution_link):
        query = """
        MERGE (p:Pokemon {id: $pokemon_id})
        MERGE (evo:Pokemon {name: $evolution_name})
        MERGE (p)-[:EVOLVES_INTO]->(evo)
        """
        self.session.run(query, pokemon_id=pokemon_id, evolution_name=evolution_name)

    def insert_skill(self, pokemon_id, skill_name, skill_link):
        query = """
        MERGE (p:Pokemon {id: $pokemon_id})
        MERGE (s:Skill {name: $skill_name})
        MERGE (p)-[:CAN_LEARN]->(s)
        """
        self.session.run(query, pokemon_id=pokemon_id, skill_name=skill_name)

    def closed(self, reason):
        self.session.close()
    
    def run_query(self, query):
        result = self.session.run(query1)
        self.run_query(query1)
        query1 = """
MATCH (p:Pokemon {name: 'Pikachu'})-[:HAS_TYPE]->(t:Type)<-[:HAS_TYPE]-(attackers:Pokemon)
WHERE p.weight > 10
RETURN attackers.name
"""
        for record in result:
            print(record)

query = """
MATCH (p:Pokemon {name: 'Pikachu'})-[:HAS_TYPE]->(t:Type)<-[:HAS_TYPE]-(attackers:Pokemon)
WHERE p.weight > 10
RETURN attackers.name
"""

query = """
MATCH (p:Pokemon)-[:HAS_TYPE]->(t:Type)-[:WEAK_AGAINST]->(p2:Pokemon)
WHERE t.name = 'Gelo'
RETURN t.name, COUNT(p) AS count ORDER BY count DESC LIMIT 1
"""

query = """
MATCH (p:Pokemon)-[:EVOLVES_INTO]->(evo1:Pokemon)-[:EVOLVES_INTO]->(evo2:Pokemon)
WHERE evo2.weight >= p.weight * 2
RETURN COUNT(evo2) AS evolution_count
"""