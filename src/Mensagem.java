import java.io.Serializable;

class Mensagem implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public enum TipoMensagem {
        PUT, GET, PUT_OK, REPLICATION, REPLICATION_OK, WAIT_FOR_RESPONSE, GET_RESPONSE
    }
    
    private TipoMensagem tipo;
    private String key;
    private String value;
    private long timestamp;
    private String clienteIP;
    private int clientePorta;
    
    public Mensagem(TipoMensagem tipo, String key, String value, long timestamp) {
        this.tipo = tipo;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    public Mensagem(TipoMensagem tipo, String key, String value) {
        this(tipo, key, value, 0);
    }
    
    public Mensagem(TipoMensagem tipo, String key, long timestamp) {
        this(tipo, key, null, timestamp);
    }
    
    public Mensagem(TipoMensagem tipo) {
        this(tipo, null, null, 0);
    }
    
    // Getters e Setters
    public TipoMensagem getTipo() { return tipo; }
    public void setTipo(TipoMensagem tipo) { this.tipo = tipo; }
    
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    
    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public String getClienteIP() { return clienteIP; }
    public void setClienteIP(String clienteIP) { this.clienteIP = clienteIP; }
    
    public int getClientePorta() { return clientePorta; }
    public void setClientePorta(int clientePorta) { this.clientePorta = clientePorta; }
}