/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.StringParser;
import edu.umass.cs.gnscommon.utils.StringUtil;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Command to update the entire records using JSON
 */
public class Update extends ConsoleCommand {

  /**
   * Creates a new <code>Update</code> object
   *
   * @param module
   */
  public Update(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Update the value of the target GUID using the JSON String (using the credential of the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "update";
  }

  @Override
  public String getCommandParameters() {
    return "[target_guid_or_alias] jsonString";
  }

  /**
   * Override execute to check for a selected gui
   *
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception {
    if (!module.isCurrentGuidSetAndVerified()) {
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    GNSClientCommands gnsClient = module.getGnsClient();
    try {
      StringParser st = new StringParser(commandText.trim());
      String guid;
      switch (st.countTokens()) {
        case 1:
          guid = module.getCurrentGuid().getGuid();
          break;
        case 2:
          guid = st.nextToken();
          if (!StringUtil.isValidGuidString(guid)) {
            // We probably have an alias, lookup the GUID
            guid = gnsClient.lookupGuid(guid);
          }
          break;
        default:
          wrongArguments();
          return;
      }
      String value = st.nextToken();
      JSONObject json = new JSONObject(value);

      gnsClient.update(guid, json, module.getCurrentGuid());
      console.printString("GUID " + guid + " has been updated using '" + json.toString());
      console.printNewline();
    } catch (IOException | ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    } catch (JSONException e) {
      console.printString("Unable to parse JSON string: " + e + "\n");
    }
  }
}
