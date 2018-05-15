public class valid {
    private int x;
    
    valid() {
        x = 0;
    }
    
    void printX(){
        System.out.println(x);
    }
    
    public static void main(String[] args){
        valid v = new valid();
        v.printX();
    }
    
}
