using Api.CommonLib.Models;

namespace Api.CommonLib.Interfaces
{
    public interface IUserRepository
    {
        public Response AddUser(UserModel user);
        public Response FindUser(string userId);
        public Task<Response> AddUserAsync(UserModel user);
    }
}