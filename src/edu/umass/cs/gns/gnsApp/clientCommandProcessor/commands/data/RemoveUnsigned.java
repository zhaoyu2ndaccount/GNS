/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 *
 * @author westy
 */
public class RemoveUnsigned extends AbstractUpdate {

  /**
   *
   * @param module
   */
  public RemoveUnsigned(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_REMOVE;
  }

  @Override
  public String getCommandName() {
    return REMOVE;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE};
  }

  @Override
  public String getCommandDescription() {
    return "Removes the value from the key value pair for the given GUID."
            + " Field must be world writeable as this command does not specify the writer and is not signed.";
  }
}