/*
* Copyright 2013 Produban
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.produban.openbus.processor.register;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Keep the records in the database realtime openTSDB    
 */
public class OpenTSDBRecoder implements RemoteRecoder {

	Socket sock = null;
	
	@Override
	public void init(String host, int port) throws UnknownHostException, IOException {
		sock = new Socket(host, port);		
	}

	@Override
	public void send(String message) throws IOException {
		sock.getOutputStream().write(message.getBytes()); 		
	}

	@Override
	public void close() throws IOException {
		sock.close();
	}
}
