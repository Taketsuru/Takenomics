package jp.dip.myuminecraft.takenomics;

import jp.dip.myuminecraft.takenomics.models.ShopTable.Shop;

import org.bukkit.block.BlockState;

public interface ShopValidator {

    public Shop validate(BlockState state);

}
