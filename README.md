# Roles

The **Roles** module is designed to manage roles, streamline vetting processes, and monitor member activities within a Discord server.

## Features

- Assign or remove custom roles using slash commands and context menus.
- Maintain a dynamic list of assignable roles that can be updated in real-time.
- Create vetting threads automatically in designated forums for new members.
- Enable members with specific roles to approve or reject vetting requests through interactive buttons.
- Track approval and rejection counts to determine member eligibility.
- Display a comprehensive, paginated list of all servant roles and their respective members.
- Enable quick navigation and search functionality within the servant directory.
- Incarcerate members for specified durations, restricting access to server features.
- Manually release incarcerated members before the end of their restriction period.
- Automatically handle role assignments and removals upon incarceration and release.
- Monitor user activity and automatically update roles based on predefined thresholds.
- Enhance member engagement by rewarding active participation.
- Log all role assignments, removals, and system-triggered actions to designated channels.
- Ensure accountability and provide audit trails for moderation activities.

## Usage

### Slash Commands

- `/roles custom configure`: Add or remove custom roles.
  - Options:
    - `roles` (string, required): Comma-separated list of roles to add or remove.
    - `action` (string, required): Choose between `add` or `remove`.
- `/roles vetting assign`: Assign vetting roles to a member.
  - Options:
    - `member` (user, required): The member to assign roles to.
    - `ideology` (string, optional): Specify ideology role.
    - `domicile` (string, optional): Specify domicile role.
    - `status` (string, optional): Specify status role.
- `/roles vetting remove`: Remove vetting roles from a member.
  - Options:
    - `member` (user, required): The member to remove roles from.
    - `ideology` (string, optional): Specify ideology role.
    - `domicile` (string, optional): Specify domicile role.
    - `status` (string, optional): Specify status role.
- `/roles servant view`: Display a paginated list of all servant roles and their members.
- `/roles penitentiary incarcerate`: Incarcerate a member for a specified duration.
  - Options:
    - `member` (user, required): The member to incarcerate.
    - `duration` (string, required): Duration format (e.g., `1d 2h 30m`).
- `/roles penitentiary release`: Manually release an incarcerated member.
  - Options:
    - `member` (user, required): The member to release.

### Context Menus

- User Context Menu
  - **Custom Roles**: Assign custom roles directly.

## Configuration

Customization is facilitated through the `Config` and `Data` classes within `main.py`. Key configuration settings include:

- `VETTING_FORUM_ID`: Discord channel ID for vetting threads.
- `VETTING_ROLE_IDS`: List of role IDs authorized to participate in vetting.
- `REQUIRED_APPROVALS`: Number of approvals needed to grant roles.
- `REQUIRED_REJECTIONS`: Number of rejections needed to deny roles.
- `REJECTION_WINDOW_DAYS`: Timeframe to allow rejections after approval.
- `ELECTORAL_ROLE_ID`: Role granted upon successful vetting.
- `APPROVED_ROLE_ID`: Role for approved members.
- `TEMPORARY_ROLE_ID`: Role for temporarily restricted members.
- `INCARCERATED_ROLE_ID`: Role assigned to incarcerated members.
- `AUTHORIZED_CUSTOM_ROLE_IDS`: Roles permitted to manage custom roles.
- `AUTHORIZED_PENITENTIARY_ROLE_IDS`: Roles permitted to use penitentiary commands.
- `LOG_FORUM_ID`: Discord forum channel ID for logging actions.
- `LOG_POST_ID`: Specific post ID within the log forum for detailed logs.
- `GUILD_ID`: Discord server (guild) ID where the module operates.
- Parameters such as `MESSAGE_RATE_WINDOW`, `NON_ASCII_THRESHOLD`, `DIGIT_THRESHOLD`, and `MIN_ENTROPY_THRESHOLD` are adjustable to fine-tune moderation sensitivity based on member activity.
