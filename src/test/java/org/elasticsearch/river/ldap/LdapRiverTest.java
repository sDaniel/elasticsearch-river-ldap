package org.elasticsearch.river.ldap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifs;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

@RunWith(FrameworkRunner.class)
@CreateDS(allowAnonAccess = true, name = "AddIT-class", partitions =
    {
        @CreatePartition(
            name = "example",
            suffix = "dc=example,dc=com",
            contextEntry = @ContextEntry(
                entryLdif = "dn: dc=example,dc=com\n" +
                    "dc: example\n" +
                    "objectClass: top\n" +
                    "objectClass: domain\n\n"),
            indexes =
                {
                    @CreateIndex(attribute = "objectClass"),
                    @CreateIndex(attribute = "dc"),
                    @CreateIndex(attribute = "ou")
            })})
@CreateLdapServer(name = "ADDIT", transports =
    { @CreateTransport(protocol = "LDAP", port = 9389, address = "localhost") })
@ApplyLdifs(
    {
        // Entry # 0
        "dn: cn=The Person,ou=system",
        "objectClass: person",
        "objectClass: top",
        "cn: The Person",
        "description: this is a person",
        "sn: Person",

        // Entry # 1
        "dn: uid=john,ou=users,ou=system",
        "objectClass: uidObject",
        "objectClass: person",
        "objectClass: top",
        "uid: john",
        "cn: John Woo",
        "sn: john",
        
        // Entry # 2
        "dn: uid=christopher,ou=users,ou=system",
        "objectClass: uidObject",
        "objectClass: person",
        "objectClass: top",
        "uid: christopher",
        "cn: Christopher Nolan",
        "sn: christopher",

        // Entry # 3
        "dn: uid=clint,ou=users,ou=system",
        "objectClass: uidObject",
        "objectClass: person",
        "objectClass: top",
        "uid: clint",
        "cn: Clint Eastwood",
        "sn: clint",
        
        // Ignored entries
        "dn: ou=Computers,uid=clint,ou=users,ou=system",
        "objectClass: organizationalUnit",
        "objectClass: top",
        "ou: computers",
        "description: Computers for Clint",
        "seeAlso: ou=Machines,uid=clint,ou=users,ou=system",

        "dn: uid=clintref,ou=users,ou=system",
        "objectClass: uidObject",
        "objectClass: referral",
        "objectClass: top",
        "uid: clintref",
        "ref: ldap://localhost:10389/uid=clint,ou=users,ou=system",
        "ref: ldap://foo:10389/uid=clint,ou=users,ou=system",
        "ref: ldap://bar:10389/uid=clint,ou=users,ou=system" })
public class LdapRiverTest extends AbstractLdapTestUnit {

	private static final String TYPE_PERSON = "person";
	private static final String LDAPSERVER = "ldapserver0";
	Node node = null;

	private Client client;
	
	@Before
	public void setUp() throws Exception{
		node = NodeBuilder.nodeBuilder().local(true).node();
		client = node.client();
		
		try {
			client.admin().indices().prepareDelete("_river", LDAPSERVER).execute().actionGet();
		} catch (IndexMissingException e) {
			// Ok
		}
		
		Thread.sleep(1000);
		
		XContentBuilder xb = jsonBuilder()
								.startObject()
									.field("type", "ldap")
									.startObject("ldap")
										.field("host", "localhost")
										.field("port", 9389)
										.field("ssl", false)
										.field("userDn", "uid=admin,ou=system")
										.field("credentials", "secret")
										.field("filter", "(objectClass=person)")
										.field("baseDn", "ou=system")
										.field("scope", "")
										.field("attributes", new String[]{"sn", "cn", "objectClass"})
										.field("fields", new String[]{"_id", "name", "groups"} )
										.field("poll", 60000)
									.endObject()
									.startObject("index")
										.field("index", LDAPSERVER)
										.field("type", TYPE_PERSON)
									.endObject()
								.endObject();
		
		client.prepareIndex("_river", "my_ldap_river", "_meta").setRefresh(true).setSource(xb).execute().actionGet();
		
		
		// Wait for river indexation
		Thread.sleep(1000);
	}
	
	@Test
    public void testLdapRiverResultCount() throws Exception {
		// 4 from sample data + 1 default LDAP administrator user
		CountResponse resultsCount = client.prepareCount(LDAPSERVER).setQuery(QueryBuilders.termQuery("_type", TYPE_PERSON)).execute().actionGet();
		assertEquals(5L, resultsCount.getCount());
    }

	@Test
    public void testSearchGroups() throws Exception {
		SearchResponse results = client.prepareSearch(LDAPSERVER).setTypes(TYPE_PERSON).setQuery(QueryBuilders.matchQuery("groups", "uidObject")).execute().actionGet();
		assertEquals(3L, results.getHits().totalHits());
    }

	@Test
    public void testSearchUniqueResult() throws Exception {
		// But Clint is definitley unique
		assertEquals(1L, client.prepareSearch(LDAPSERVER).setTypes(TYPE_PERSON).setQuery(QueryBuilders.matchQuery("name", "clint")).execute().actionGet().getHits().totalHits());
		assertEquals(1L, client.prepareSearch(LDAPSERVER).setTypes(TYPE_PERSON).setQuery(QueryBuilders.queryString("woo")).execute().actionGet().getHits().totalHits());
		assertEquals(2, client.prepareMultiGet().add(LDAPSERVER, TYPE_PERSON, "christopher").add(LDAPSERVER, TYPE_PERSON, "john").execute().actionGet().getResponses().length);
    }
	
	@After
	public void tearDown(){
		node.close();
	}
}
