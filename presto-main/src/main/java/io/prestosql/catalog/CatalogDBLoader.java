/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.mysql.jdbc.Driver;
import io.airlift.json.JsonCodec;
import io.prestosql.metadata.DynamicCatalogStoreConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static io.airlift.json.JsonCodec.jsonCodec;

public class CatalogDBLoader
        extends CatalogLoader
{
    public CatalogDBLoader(DynamicCatalogStoreConfig config)
    {
        super(config);
    }

    @Override
    public ImmutableMap<String, CatalogInfo> load()
            throws Exception
    {
        ImmutableMap.Builder builder = ImmutableMap.builder();
        String sql = "SELECT * FROM catalog";
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            JsonCodec<Map> mapJsonCodec = jsonCodec(Map.class);
            JsonCodec<CatalogInfo> catalogJsonCodec = jsonCodec(CatalogInfo.class);
            while (resultSet.next()) {
                String catalogName = resultSet.getString("catalog_name");
                String connectorName = resultSet.getString("connector_name");
                String properties = replaceVariable(resultSet.getString("properties"));
                if (!Strings.isNullOrEmpty(catalogName)
                        && !Strings.isNullOrEmpty(connectorName)
                        && !Strings.isNullOrEmpty(properties)) {
                    CatalogInfo catalogInfo = new CatalogInfo(catalogName, connectorName, mapJsonCodec.fromJson(properties));
                    String md5 = Hashing.md5().hashBytes(catalogJsonCodec.toJsonBytes(catalogInfo)).toString();
                    builder.put(md5, catalogInfo);
                }
            }
        }
        return builder.build();
    }

    private Connection openConnection()
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties();
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        connectionProperties.setProperty("useSSL", "false");

        return new Driver().connect(config.getCatalogSourceMysqlUrl(), connectionProperties);
    }

    private Properties basicConnectionProperties()
    {
        Properties connectionProperties = new Properties();
        if (config.getCatalogSourceMysqlUser() != null) {
            connectionProperties.setProperty("user", config.getCatalogSourceMysqlUser());
        }
        if (config.getCatalogSourceMysqlPassword() != null) {
            connectionProperties.setProperty("password", config.getCatalogSourceMysqlPassword());
        }
        return connectionProperties;
    }
}
