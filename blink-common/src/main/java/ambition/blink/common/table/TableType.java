package ambition.blink.common.table;

/**
 * @Author: wpl
 */
public enum TableType {
   NONE(0),
   SOURCE(1),
   SINK(2),
   SIDE(3);

   int value;

   TableType(int value) {
      this.value = value;
   }

   public int getValue() {
      return this.value;
   }

   public static TableType valueOf(int value) {
      switch (value) {
         case 0:
            return NONE;
         case 1:
            return SOURCE;
         case 2:
            return SINK;
         case 3:
            return SIDE;
         default:
            return NONE;
      }
   }
}