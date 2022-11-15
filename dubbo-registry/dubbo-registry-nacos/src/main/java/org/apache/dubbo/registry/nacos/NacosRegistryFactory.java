/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dubbo.registry.nacos;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;

import static org.apache.dubbo.common.constants.CommonConstants.CONFIG_NAMESPACE_KEY;
import static org.apache.dubbo.registry.nacos.util.NacosNamingServiceUtils.createNamingService;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;

/**
 * Nacos {@link RegistryFactory}
 *
 * @since 2.6.5
 */
public class NacosRegistryFactory extends AbstractRegistryFactory {
    
    @Override
    public Registry getRegistry(URL url) {
        URL providerUrl = getProviderUrl(url);
        if (providerUrl != null) {
            String port = providerUrl.getParameter("bind.port");
            url = url.addParameter(CommonConstants.PROTOCOL_KEY, providerUrl.getProtocol());
            url = url.addParameter(CommonConstants.PORT_KEY, port);
        }
        return super.getRegistry(url);
    }
    
    @Override
    protected String createRegistryCacheKey(URL url) {
        String namespace = url.getParameter(CONFIG_NAMESPACE_KEY);
        String protocol = url.getParameter(CommonConstants.PROTOCOL_KEY);
        String port = url.getParameter(CommonConstants.PORT_KEY);
        url = URL.valueOf(url.toServiceStringWithoutResolving());
        if (StringUtils.isNotEmpty(namespace) && !CommonConstants.DUBBO.equals(namespace)) {
            // ignore "dubbo" namespace, make the behavior equivalent to configcenter
            url = url.addParameter(CONFIG_NAMESPACE_KEY, namespace);
        }
        url = url.addParameter(CommonConstants.PROTOCOL_KEY, protocol);
        url = url.addParameter(CommonConstants.PORT_KEY, port);
        return url.toFullString();
    }
    
    @Override
    protected Registry createRegistry(URL url) {
        URL nacosURL = url;
        if (CommonConstants.DUBBO.equals(url.getParameter(CONFIG_NAMESPACE_KEY))) {
            // ignore "dubbo" namespace, make the behavior equivalent to configcenter
            nacosURL = url.removeParameter(CONFIG_NAMESPACE_KEY);
        }
        return new NacosRegistry(nacosURL, createNamingService(nacosURL));
    }
    
    private URL getProviderUrl(URL url) {
        String export = url.getParameterAndDecoded(EXPORT_KEY);
        if (export == null || export.length() == 0) {
            return null;
        }
        return URL.valueOf(export);
    }
}
