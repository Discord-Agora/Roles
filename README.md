# Roles

The **Roles** module is designed to manage roles, streamline vetting processes, and monitor member activities within a Discord server.

## Features

- Assign or remove custom roles using slash commands and context menus
- Maintain a dynamic list of assignable roles that can be updated in real-time
- Create vetting threads automatically in designated forums for new members
- Enable members with specific roles to approve or reject vetting requests through interactive buttons
- Track approval and rejection counts to determine member eligibility
- Display a comprehensive, paginated list of all servant roles and their respective members
- Enable quick navigation and search functionality within the servant directory
- Incarcerate members for specified durations, restricting access to server features
- Manually release incarcerated members before the end of their restriction period
- Automatically handle role assignments and removals upon incarceration and release
- Monitor user activity and automatically update roles based on predefined thresholds
- Enhance member engagement by rewarding active participation
- Log all role assignments, removals, and system-triggered actions to designated channels
- Ensure accountability and provide audit trails for moderation activities
- Advanced message analysis system to detect spam and low-quality content
- Dynamic threshold adjustment based on user behavior and message patterns
- Automatic role progression based on activity and message quality
- Intelligent message quality assessment using entropy and pattern analysis
- Automatic conflict resolution for role assignments
- Recovery streak system to reward consistent good behavior
- Reaction-based role management system with customizable actions
- Automatic role assignment/removal based on emoji reactions

## Usage

### Slash Commands

- `/roles custom configure`: Add or remove custom roles
  - Options:
    - `roles` (string, required): Comma-separated list of roles to add or remove
    - `action` (string, required): Choose between `add` or `remove`

- `/roles custom mention`: Mention members with specific custom roles
  - Options:
    - `roles` (string, required): Role names to mention members from

- `/roles vetting toggle`: Configure message monitoring and validation settings
  - Options:
    - `type` (string, required): Setting type to configure
    - `state` (string, required): Enable or disable the setting

- `/roles vetting assign`: Assign vetting roles to a member
  - Options:
    - `member` (user, required): The member to assign roles to
    - `ideology` (string, optional): Specify ideology role
    - `domicile` (string, optional): Specify domicile role
    - `status` (string, optional): Specify status role
    - `others` (string, optional): Specify other roles

- `/roles vetting remove`: Remove vetting roles from a member
  - Options:
    - `member` (user, required): The member to remove roles from
    - `ideology` (string, optional): Specify ideology role
    - `domicile` (string, optional): Specify domicile role
    - `status` (string, optional): Specify status role
    - `others` (string, optional): Specify other roles

- `/roles servant view`: Display a paginated list of all servant roles and their members

- `/roles penitentiary incarcerate`: Incarcerate a member for a specified duration
  - Options:
    - `member` (user, required): The member to incarcerate
    - `duration` (string, required): Duration format (e.g., `1d 2h 30m`)

- `/roles penitentiary release`: Manually release an incarcerated member
  - Options:
    - `member` (user, required): The member to release

- `/roles reaction start`: Configure reaction roles
  - Options:
    - `message` (string, required): Message ID or URL to monitor
    - `emoji` (string, required): Emoji to react with
    - `role` (role, required): Role to assign/remove
    - `action` (string, required): Whether to add or remove the role

- `/roles reaction stop`: Stop monitoring a reaction
  - Options:
    - `config` (string, required): Select reaction configuration to stop

- `/roles debug view`: View configuration settings and statistics
  - Options:
    - `config` (string, required): Configuration type to view (vetting/custom/incarcerated/stats/dynamic)

- `/roles debug export`: Export files from the extension directory
  - Options:
    - `type` (string, required): Type of files to export

- `/roles debug inactive`: Convert inactive members to missing members
  - Requires administrator permissions
  - Automatically identifies and converts inactive members after 15 days

- `/roles debug conflicts`: Check and resolve role conflicts
  - Requires administrator permissions
  - Automatically resolves conflicting role assignments based on role priorities

### Context Menus

- User Context Menu
  - `Custom Roles`: Assign or remove custom roles directly through an interactive menu

## Configuration

Key configuration options include:

- `ELECT_VETTING_FORUM_ID`: Discord forum ID for electoral vetting threads
- `APPR_VETTING_FORUM_ID`: Discord forum ID for approval vetting threads
- `VETTING_ROLE_IDS`: List of role IDs authorized to participate in vetting
- `ELECTORAL_ROLE_ID`: Role granted upon successful electoral vetting
- `APPROVED_ROLE_ID`: Role for approved members
- `TEMPORARY_ROLE_ID`: Role for temporarily restricted members
- `MINISTER_ROLE_ID`: Role authorized to configure reaction roles
- `MISSING_ROLE_ID`: Role for inactive/missing members
- `INCARCERATED_ROLE_ID`: Role assigned to incarcerated members
- `AUTHORIZED_CUSTOM_ROLE_IDS`: Roles permitted to manage custom roles
- `AUTHORIZED_PENITENTIARY_ROLE_IDS`: Roles permitted to use penitentiary commands
- `REQUIRED_APPROVALS`: Number of approvals needed to grant roles
- `REQUIRED_REJECTIONS`: Number of rejections needed to deny roles
- `REJECTION_WINDOW_DAYS`: Timeframe to allow rejections after approval
- `LOG_CHANNEL_ID`: Main logging channel
- `LOG_FORUM_ID`: Forum channel for detailed logs
- `LOG_POST_ID`: Specific post ID for detailed logs
- `GUILD_ID`: Discord server (guild) ID
- `MESSAGE_RATE_WINDOW`: Time window for message rate calculation
- `MAX_REPETITIONS`: Maximum allowed message repetitions
- `DIGIT_THRESHOLD`: Threshold for excessive digit usage
- `MIN_ENTROPY_THRESHOLD`: Minimum required message entropy

### Files

- `stats.json`: User statistics and feedback scores
- `custom.json`: Custom role configurations
- `vetting.json`: Vetting role settings
- `incarcerated_members.json`: Incarcerated member records and status
- `reaction_roles.json`: Reaction role configurations and mappings

### Algorithm

1. Role Vetting
   - Message:
     - Maintains a sliding 2-hour window of message timestamps
     - Computes Shannon entropy and numerical character density
     - Identifies message duplication and spam patterns
     - Implements language-specific processing for CJK vs Latin text
   - Threshold:
     - Core parameters:
     - `MIN_MESSAGE_ENTROPY`: `1.5` (valid range: `0.0-4.0`)
     - `DIGIT_RATIO_THRESHOLD`: `0.5` (valid range: `0.1-1.0`)
     - Dynamic adjustment coefficients:
     - User reputation factor: `0.01 × score × tanh(|score|/5)`
     - Temporal decay function: `exp(-Δt/3600)`
     - Content length normalization: `log2(max(length,2))/10`
     - CJK text coefficient: `0.7-0.8 × baseline`
   - User Metrics:
     - Reputation score (bounded `[-5.0, 5.0]`)
     - Consecutive compliance streaks
     - Violation frequency counter
     - Message timing distribution
     - Threshold recalibration timestamp
   - Adaptive Control:
     - Positive reinforcement for sustained compliance
     - Progressive penalty scaling for infractions
     - Time-based threshold regression to defaults
     - Per-user calibration state persistence
