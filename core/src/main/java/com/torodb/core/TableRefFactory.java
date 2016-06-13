
package com.torodb.core;

/**
 *
 */
public interface TableRefFactory {

    /**
     * 
     * @return
     */
    public TableRef createRoot();

    /**
     * 
     * @param name
     * @return
     */
    public TableRef createChild(TableRef parent, String name);
    
    /**
     * 
     * @param arrayDimension
     * @return
     */
    public TableRef createChild(TableRef parent, int arrayDimension);
}
