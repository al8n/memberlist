/* Memory layout for the QEMU `mps2-an386` machine (ARM MPS2 with the AN386
 * FPGA image, a Cortex-M4).
 *
 * The board exposes two 4 MB ZBT SSRAM blocks: SSRAM1 at 0x0000_0000 is used in
 * lieu of flash and is where QEMU's `-kernel` loader places the image, and
 * SSRAM2&3 at 0x2000_0000 is the main RAM. Either block is far larger than a
 * real microcontroller's, which is exactly why this machine is chosen for the
 * two-node proof: two embassy-net stacks, two engines, and the heap fit
 * comfortably. (Addresses per QEMU's hw/arm/mps2.c for AN386.)
 */
MEMORY
{
  FLASH : ORIGIN = 0x00000000, LENGTH = 4M
  RAM   : ORIGIN = 0x20000000, LENGTH = 4M
}
