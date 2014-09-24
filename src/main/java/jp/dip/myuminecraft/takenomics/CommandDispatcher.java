package jp.dip.myuminecraft.takenomics;

import java.util.HashMap;
import java.util.Map;

import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

public class CommandDispatcher implements CommandExecutor {
    
    class CommandInfo {
        CommandExecutor executor;
        String          permission;
        
        CommandInfo(CommandExecutor executor, String permission) {
            this.executor = executor;
            this.permission = permission;
        }
    }

    private Messages messages;
    private int commandPosition;
    private String permissionPrefix;
    private Map<String, CommandInfo> table = new HashMap<String, CommandInfo>();

    public CommandDispatcher(Messages messages, String permissionPrefix) {
        this.messages = messages;
        this.commandPosition = 0;
        this.permissionPrefix = permissionPrefix.toLowerCase();
    }
    
    public CommandDispatcher(CommandDispatcher parent, String subcommand) {
        this.messages = parent.messages;
        this.commandPosition = parent.commandPosition + 1;
        this.permissionPrefix = parent.permissionPrefix + subcommand.toLowerCase() + ".";
        parent.addCommand(subcommand, this);
    }
    
    public int getCommandPosition() {
        return commandPosition;
    }

    public void addCommand(String subcommand, CommandExecutor executor) {
        assert ! table.containsKey(subcommand);
        String permission = permissionPrefix + subcommand.toLowerCase();
        table.put(subcommand, new CommandInfo(executor, permission));
    }
    
    public String getCommandString(Command cmd, String[] args) {
        StringBuffer buffer = new StringBuffer(cmd.getName());
        for (int i = 0; i <= commandPosition; ++i) {
            buffer.append(' ');
            buffer.append(args[i]);
        }
        return buffer.toString();
    }

    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
        if (args.length <= commandPosition) {
            return false;
        }

        String subcommand = args[commandPosition];
        CommandInfo info = table.get(subcommand);
        if (info == null) {
            return false;
        }

        if (! sender.isOp() && sender instanceof Player
                && ! ((Player)sender).hasPermission(info.permission)) {
            String cmdStr = getCommandString(cmd, args);
            messages.send((Player)sender, "noPermissionToRunCommand", cmdStr);

            return true;
        }

        return info.executor.onCommand(sender, cmd, label, args);
    }
}
